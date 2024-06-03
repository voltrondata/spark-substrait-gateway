# SPDX-License-Identifier: Apache-2.0
"""SparkConnect server that drives a backend using Substrait."""
import io
import logging
from collections.abc import Generator
from concurrent import futures

import grpc
import pyarrow as pa
import pyspark.sql.connect.proto.base_pb2 as pb2
import pyspark.sql.connect.proto.base_pb2_grpc as pb2_grpc
from google.protobuf.json_format import MessageToJson
from grpc_channelz.v1 import channelz
from pyspark.sql.connect.proto import types_pb2
from substrait.gen.proto import plan_pb2

from gateway.backends.backend import Backend
from gateway.backends.backend_options import BackendEngine, BackendOptions
from gateway.backends.backend_selector import find_backend
from gateway.converter.conversion_options import arrow, datafusion, duck_db
from gateway.converter.spark_to_substrait import SparkSubstraitConverter

from src.gateway.converter.conversion_options import ConversionOptions

_LOGGER = logging.getLogger(__name__)


def show_string(table: pa.lib.Table) -> bytes:
    """Convert a table into a byte serialized single row string column Arrow Table."""
    results_str = str(table)
    schema = pa.schema([('show_string', pa.string())])
    array = pa.array([results_str])
    batch = pa.RecordBatch.from_arrays([array], schema=schema)
    result_table = pa.Table.from_batches([batch])
    buffer = io.BytesIO()
    stream = pa.RecordBatchStreamWriter(buffer, schema)
    stream.write_table(result_table)
    stream.close()
    return buffer.getvalue()


def batch_to_bytes(batch: pa.RecordBatch, schema: pa.Schema) -> bytes:
    """Serialize a RecordBatch into a bytes."""
    result_table = pa.Table.from_batches(batches=[batch])
    buffer = io.BytesIO()
    stream = pa.RecordBatchStreamWriter(buffer, schema)
    stream.write_table(result_table)
    stream.close()
    return buffer.getvalue()


def convert_pyarrow_datatype_to_spark(arrow_type: pa.DataType) -> types_pb2.DataType:
    """Convert an Arrow datatype into a Spark type."""
    if arrow_type == pa.bool_():
        data_type = types_pb2.DataType(boolean=types_pb2.DataType.Boolean())
    elif arrow_type == pa.int8():
        data_type = types_pb2.DataType(byte=types_pb2.DataType.Byte())
    elif arrow_type == pa.int16():
        data_type = types_pb2.DataType(short=types_pb2.DataType.Short())
    elif arrow_type == pa.int32():
        data_type = types_pb2.DataType(integer=types_pb2.DataType.Integer())
    elif arrow_type == pa.int64():
        data_type = types_pb2.DataType(long=types_pb2.DataType.Long())
    elif arrow_type == pa.float32():
        data_type = types_pb2.DataType(float=types_pb2.DataType.Float())
    elif arrow_type == pa.float64():
        data_type = types_pb2.DataType(double=types_pb2.DataType.Double())
    elif arrow_type == pa.string():
        data_type = types_pb2.DataType(string=types_pb2.DataType.String())
    elif arrow_type == pa.timestamp('us'):
        data_type = types_pb2.DataType(timestamp=types_pb2.DataType.Timestamp())
    elif arrow_type == pa.date32():
        data_type = types_pb2.DataType(date=types_pb2.DataType.Date())
    elif arrow_type == pa.null():
        data_type = types_pb2.DataType(null=types_pb2.DataType.NULL())
    elif str(arrow_type).startswith('struct'):
        field_type = types_pb2.DataType(
            struct=types_pb2.DataType.Struct())
        x: pa.StructType = arrow_type
        for i in range(x.num_fields):
            y = x.field(i)
            subfield = convert_pyarrow_datatype_to_spark(y.type)
            field_type.struct.fields.append(
                types_pb2.DataType.StructField(name=y.name, data_type=subfield,
                                               nullable=False))
        return field_type
    elif str(arrow_type).startswith('list'):
        subtype = convert_pyarrow_datatype_to_spark(arrow_type.value_type)
        data_type = types_pb2.DataType(
            array=types_pb2.DataType.Array(element_type=subtype))
    elif str(arrow_type).startswith('map'):
        key_type = convert_pyarrow_datatype_to_spark(arrow_type.key_type)
        value_type = convert_pyarrow_datatype_to_spark(arrow_type.item_type)
        data_type = types_pb2.DataType(
            map=types_pb2.DataType.Map(key_type=key_type, value_type=value_type))
    elif isinstance(arrow_type, pa.Decimal128Type):
        data_type = types_pb2.DataType(decimal=types_pb2.DataType.Decimal(
            precision=arrow_type.precision, scale=arrow_type.scale))
    else:
        raise NotImplementedError(f'Unexpected field type: {arrow_type}')

    return data_type


def convert_pyarrow_schema_to_spark(schema: pa.Schema) -> types_pb2.DataType:
    """Convert a pyarrow schema to a SparkConnect DataType.Struct schema."""
    fields = []
    for field in schema:
        fields.append(types_pb2.DataType.StructField(
            name=field.name, data_type=convert_pyarrow_datatype_to_spark(field.type),
            nullable=field.nullable))

    return types_pb2.DataType(struct=types_pb2.DataType.Struct(fields=fields))


def create_dataframe_view(rel: pb2.Plan, backend) -> None:
    """Register the temporary dataframe."""
    dataframe_view_name = rel.command.create_dataframe_view.name
    read_data_source_relation = rel.command.create_dataframe_view.input.read.data_source
    fmt = read_data_source_relation.format
    path = read_data_source_relation.paths[0]
    backend.register_table(dataframe_view_name, path, fmt)


class Statistics:
    """Statistics about the requests made to the server."""

    def __init__(self):
        """Initialize the statistics."""
        self.config_requests: int = 0
        self.analyze_requests: int = 0
        self.execute_requests: int = 0
        self.add_artifacts_requests: int = 0
        self.artifact_status_requests: int = 0
        self.interrupt_requests: int = 0
        self.reattach_requests: int = 0
        self.release_requests: int = 0
        self.database_resets: int = 0

        self.requests: list[str] = []
        self.plans: list[str] = []

    def add_request(self, request):
        """Remember a request for later introspection."""
        self.requests.append(str(request))

    def add_plan(self, plan: plan_pb2.Plan):
        """Remember a plan for later introspection."""
        self.plans.append(MessageToJson(plan))

    def reset(self):
        """Reset the statistics."""
        # TODO -- Consider removing this (and all calls to this).
        self.config_requests = 0
        self.analyze_requests = 0
        self.execute_requests = 0
        self.add_artifacts_requests = 0
        self.artifact_status_requests = 0
        self.interrupt_requests = 0
        self.reattach_requests = 0
        self.release_requests = 0
        self.database_resets = 0

        self.requests = []
        self.plans = []


class ExecutionDetails:
    """Execution related classes for a given session."""

    def __init__(self):
        self.backend: Backend | None = None
        self.sql_backend: Backend | None = None
        self.converter: SparkSubstraitConverter | None = None
        self.statistics = Statistics()

        self.failed = False

    def initialize(self, options: ConversionOptions):
        """Initialize the execution of the Plan by setting the backend."""
        if not self.backend:
            self.backend = find_backend(options.backend)
            self.sql_backend = find_backend(BackendOptions(BackendEngine.DUCKDB, False))
            self.converter = SparkSubstraitConverter(options)
            self.converter.set_backends(self.backend, self.sql_backend)

    def mark_failed(self):
        """Marks an execution as failed."""
        self.failed = True


class ExecutionFactory:
    """Provides an ExecutionDetails per session id."""

    def __init__(self):
        self._session_info: dict[str, ExecutionDetails] = {}

    def get(self, session_id: str) -> ExecutionDetails:
        if session_id not in self._session_info:
            self._session_info[session_id] = ExecutionDetails()
        execution = self._session_info.get(session_id)
        if execution.failed:
            raise EnvironmentError("Current session had a previous failure.")
        return execution


# pylint: disable=E1101,fixme
class SparkConnectService(pb2_grpc.SparkConnectServiceServicer):
    """Provides the SparkConnect service."""

    # pylint: disable=unused-argument
    def __init__(self, *args, **kwargs):
        """Initialize the SparkConnect service."""
        # This is the central point for configuring the behavior of the service.
        self._options = duck_db()

        self._execution = ExecutionFactory()

    def _ReinitializeExecution(self) -> None:
        """Reinitialize the execution of the Plan by resetting the backend."""
        self._statistics.database_resets += 1
        if not self._backend:
            return self._InitializeExecution()
        self._backend.reset_connection()
        self._sql_backend.reset_connection()
        return None

    def ExecutePlan(
            self, request: pb2.ExecutePlanRequest, context: grpc.RpcContext) -> Generator[
        pb2.ExecutePlanResponse, None, None]:
        """Execute the given plan and return the results."""
        execution = self._execution.get(request.session_id)
        execution.statistics.execute_requests += 1
        execution.statistics.add_request(request)
        _LOGGER.info('ExecutePlan: %s', request)
        execution.initialize(self._options)
        match request.plan.WhichOneof('op_type'):
            case 'root':
                substrait = execution.converter.convert_plan(request.plan)
            case 'command':
                match request.plan.command.WhichOneof('command_type'):
                    case 'sql_command':
                        if "CREATE" in request.plan.command.sql_command.sql:
                            connection = execution.backend.get_connection()
                            connection.execute(request.plan.command.sql_command.sql)
                            yield pb2.ExecutePlanResponse(
                                session_id=request.session_id,
                                result_complete=pb2.ExecutePlanResponse.ResultComplete())
                            return
                        try:
                            substrait = execution.sql_backend.convert_sql(
                                request.plan.command.sql_command.sql)
                        except Exception as err:
                            self._ReinitializeExecution()
                            raise err
                    case 'create_dataframe_view':
                        create_dataframe_view(request.plan, execution.backend)
                        create_dataframe_view(request.plan, execution.sql_backend)
                        yield pb2.ExecutePlanResponse(
                            session_id=request.session_id,
                            result_complete=pb2.ExecutePlanResponse.ResultComplete())
                        return
                    case _:
                        cmd_type = request.plan.command.WhichOneof("command_type")
                        raise NotImplementedError(f'Unsupported command type: {cmd_type}')
            case _:
                raise ValueError(f'Unknown plan type: {request.plan}')
        _LOGGER.debug('  as Substrait: %s', substrait)
        execution.statistics.add_plan(substrait)
        try:
            results = execution.backend.execute(substrait)
        except Exception as err:
            execution.mark_failed()
            self._ReinitializeExecution()
            raise err
        _LOGGER.debug('  results are: %s', results)

        if not self._options.implement_show_string and request.plan.WhichOneof(
                'op_type') == 'root' and request.plan.root.WhichOneof(
            'rel_type') == 'show_string':
            yield pb2.ExecutePlanResponse(
                session_id=request.session_id,
                arrow_batch=pb2.ExecutePlanResponse.ArrowBatch(
                    row_count=results.num_rows,
                    data=show_string(results)),
                schema=types_pb2.DataType(struct=types_pb2.DataType.Struct(
                    fields=[types_pb2.DataType.StructField(
                        name='show_string',
                        data_type=types_pb2.DataType(string=types_pb2.DataType.String()))]
                )),
            )
            return

        for batch in results.to_batches():
            yield pb2.ExecutePlanResponse(
                session_id=request.session_id,
                arrow_batch=pb2.ExecutePlanResponse.ArrowBatch(
                    row_count=batch.num_rows,
                    data=batch_to_bytes(batch, results.schema)),
                schema=convert_pyarrow_schema_to_spark(results.schema),
            )

        for option in request.request_options:
            if option.reattach_options.reattachable:
                yield pb2.ExecutePlanResponse(
                    session_id=request.session_id,
                    result_complete=pb2.ExecutePlanResponse.ResultComplete())
                return

    def AnalyzePlan(self, request, context):
        """Analyze the given plan and return the results."""
        execution = self._execution.get(request.session_id)
        execution.statistics.analyze_requests += 1
        execution.statistics.add_request(request)
        _LOGGER.info('AnalyzePlan: %s', request)
        execution.initialize(self._options)
        if request.schema:
            substrait = execution.converter.convert_plan(request.schema.plan)
            execution.statistics.add_plan(substrait)
            try:
                results = execution.backend.execute(substrait)
            except Exception as err:
                execution.mark_failed()
                self._ReinitializeExecution()
                raise err
            _LOGGER.debug('  results are: %s', results)
            return pb2.AnalyzePlanResponse(
                session_id=request.session_id,
                schema=pb2.AnalyzePlanResponse.Schema(schema=convert_pyarrow_schema_to_spark(
                    results.schema)))
        raise NotImplementedError('AnalyzePlan not yet implemented for non-Schema requests.')

    def Config(self, request, context):
        """Get or set the configuration of the server."""
        execution = self._execution.get(request.session_id)
        execution.statistics.config_requests += 1
        _LOGGER.info('Config: %s', request)
        response = pb2.ConfigResponse(session_id=request.session_id)
        match request.operation.WhichOneof('op_type'):
            case 'set':
                for pair in request.operation.set.pairs:
                    if pair.key == 'spark-substrait-gateway.backend':
                        # Set the server backend for all connections (including ongoing ones).
                        match pair.value:
                            case 'arrow':
                                self._options = arrow()
                            case 'duckdb':
                                self._options = duck_db()
                            case 'datafusion':
                                self._options = datafusion()
                            case _:
                                raise ValueError(f'Unknown backend: {pair.value}')
                response.pairs.extend(request.operation.set.pairs)
            case 'get':
                for key in request.operation.get.keys:
                    if key == 'spark-substrait-gateway.backend':
                        response.pairs.add(key=key, value=str(self._options.backend.backend))
                    elif key == 'spark-substrait-gateway.plan_count':
                        response.pairs.add(key=key, value=str(len(execution.statistics.plans)))
                    elif key.startswith('spark-substrait-gateway.plan.'):
                        index = int(key[len('spark-substrait-gateway.plan.'):])
                        if 0 <= index - 1 < len(execution.statistics.plans):
                            response.pairs.add(key=key, value=execution.statistics.plans[index - 1])
                    elif key == 'spark.sql.session.timeZone':
                        response.pairs.add(key=key, value='UTC')
                    elif key in ['spark.sql.pyspark.inferNestedDictAsStruct.enabled',
                                 'spark.sql.repl.eagerEval.enabled',
                                 'spark.sql.repl.eagerEval.truncate',
                                 'spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled']:
                        response.pairs.add(key=key, value='false')
                    elif key == 'spark.sql.timestampType':
                        response.pairs.add(key=key, value='TIMESTAMP_NTZ')
                    elif key in ['spark.sql.session.localRelationCacheThreshold',
                                 'spark.sql.repl.eagerEval.maxNumRows']:
                        response.pairs.add(key=key, value='9999')
                    elif key == 'spark-substrait-gateway.use_duckdb_struct_name_behavior':
                        response.pairs.add(
                            key=key, value=str(self._options.use_duckdb_struct_name_behavior))
                    else:
                        _LOGGER.info(f'Unknown config item: {key}')
                        raise NotImplementedError(f'Unknown config item: {key}')
            case 'get_with_default':
                for pair in request.operation.get_with_default.pairs:
                    if pair.key == 'spark-substrait-gateway.backend':
                        response.pairs.add(key=pair.key, value=str(self._options.backend.backend))
                    else:
                        response.pairs.append(pair)
            case 'get_option':
                for key in request.operation.get_option.keys:
                    if key == 'spark.sql.session.timeZone':
                        response.pairs.add(key=key, value='UTC')
                    else:
                        _LOGGER.info(f'Unknown config item: {key}')
                        raise NotImplementedError(f'Unknown config item: {key}')
            case _:
                raise NotImplementedError(
                    f'Unknown config operation: {request.operation.WhichOneof("op_type")}')
        return response

    def AddArtifacts(self, request_iterator, context):
        """Add the given artifacts to the server."""
        execution = self._execution.get(request_iterator.session_id)
        execution.statistics.add_artifacts_requests += 1
        _LOGGER.info('AddArtifacts')
        response = pb2.AddArtifactsResponse()
        for request in request_iterator:
            _LOGGER.info('  batch: %s', request)
            for artifact in request.batch.artifacts:
                response.artifacts.append(pb2.AddArtifactsResponse.ArtifactSummary(
                    name=artifact.name, is_crc_successful=True
                ))
        return response

    def ArtifactStatus(self, request, context):
        """Get the status of the given artifact."""
        execution = self._execution.get(request.session_id)
        execution.statistics.artifact_status_requests += 1
        _LOGGER.info('ArtifactStatus')
        return pb2.ArtifactStatusesResponse()

    def Interrupt(self, request, context):
        """Interrupt the execution of the given plan."""
        execution = self._execution.get(request.session_id)
        execution.statistics.interrupt_requests += 1
        _LOGGER.info('Interrupt')
        return pb2.InterruptResponse()

    def ReattachExecute(
            self, request: pb2.ReattachExecuteRequest, context: grpc.RpcContext) -> Generator[
        pb2.ExecutePlanResponse, None, None]:
        """Reattach the execution of the given plan."""
        execution = self._execution.get(request.session_id)
        execution.statistics.reattach_requests += 1
        _LOGGER.info('ReattachExecute')
        yield pb2.ExecutePlanResponse(
            session_id=request.session_id,
            result_complete=pb2.ExecutePlanResponse.ResultComplete())

    def ReleaseExecute(self, request, context):
        """Release the execution of the given plan."""
        execution = self._execution.get(request.session_id)
        execution.statistics.release_requests += 1
        _LOGGER.info('ReleaseExecute')
        return pb2.ReleaseExecuteResponse()


def serve(port: int, wait: bool = True):
    """Start the SparkConnect to Substrait gateway server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_SparkConnectServiceServicer_to_server(SparkConnectService(), server)
    channelz.add_channelz_servicer(server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    if wait:
        server.wait_for_termination()
        return None
    return server


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, encoding='utf-8')
    serve(50051)
