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
from pyspark.sql.connect.proto import types_pb2
from substrait.gen.proto import algebra_pb2

from gateway.backends.backend_options import BackendOptions
from gateway.backends.backend_selector import find_backend
from gateway.converter.conversion_options import arrow, datafusion, duck_db
from gateway.converter.spark_to_substrait import SparkSubstraitConverter
from gateway.converter.sql_to_substrait import convert_sql

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


# pylint: disable=E1101
def convert_pyarrow_schema_to_spark(schema: pa.Schema) -> types_pb2.DataType:
    """Convert a pyarrow schema to a SparkConnect DataType.Struct schema."""
    fields = []
    for field in schema:
        if field.type == pa.bool_():
            data_type = types_pb2.DataType(boolean=types_pb2.DataType.Boolean())
        elif field.type == pa.int8():
            data_type = types_pb2.DataType(byte=types_pb2.DataType.Byte())
        elif field.type == pa.int16():
            data_type = types_pb2.DataType(integer=types_pb2.DataType.Short())
        elif field.type == pa.int32():
            data_type = types_pb2.DataType(integer=types_pb2.DataType.Integer())
        elif field.type == pa.int64():
            data_type = types_pb2.DataType(long=types_pb2.DataType.Long())
        elif field.type == pa.float32():
            data_type = types_pb2.DataType(float=types_pb2.DataType.Float())
        elif field.type == pa.float64():
            data_type = types_pb2.DataType(double=types_pb2.DataType.Double())
        elif field.type == pa.string():
            data_type = types_pb2.DataType(string=types_pb2.DataType.String())
        elif field.type == pa.timestamp('us'):
            data_type = types_pb2.DataType(timestamp=types_pb2.DataType.Timestamp())
        elif field.type == pa.date32():
            data_type = types_pb2.DataType(date=types_pb2.DataType.Date())
        else:
            raise NotImplementedError(
                'Conversion from Arrow schema to Spark schema not yet implemented '
                f'for type: {field.type}')

        struct_field = types_pb2.DataType.StructField(name=field.name, data_type=data_type)
        fields.append(struct_field)

    return types_pb2.DataType(struct=types_pb2.DataType.Struct(fields=fields))


def create_dataframe_view(rel: pb2.Plan, conversion_options, backend) -> algebra_pb2.Rel:
    """Register the temporary dataframe."""
    dataframe_view_name = rel.command.create_dataframe_view.name
    read_data_source_relation = rel.command.create_dataframe_view.input.read.data_source
    format = read_data_source_relation.format
    path = read_data_source_relation.paths[0]

    if not backend:
        backend = find_backend(BackendOptions(conversion_options.backend.backend, False))
    backend.register_table(dataframe_view_name, path, format)

    return backend


# pylint: disable=E1101,fixme
class SparkConnectService(pb2_grpc.SparkConnectServiceServicer):
    """Provides the SparkConnect service."""

    # pylint: disable=unused-argument
    def __init__(self, *args, **kwargs):
        """Initialize the SparkConnect service."""
        # This is the central point for configuring the behavior of the service.
        self._options = duck_db()
        self._backend_with_tempview = None
        self._tempview_session_id = None
        self._converter = None

    def ExecutePlan(
            self, request: pb2.ExecutePlanRequest, context: grpc.RpcContext) -> Generator[
        pb2.ExecutePlanResponse, None, None]:
        """Execute the given plan and return the results."""
        _LOGGER.info('ExecutePlan: %s', request)
        match request.plan.WhichOneof('op_type'):
            case 'root':
                if not self._converter and self._tempview_session_id != request.session_id:
                    self._converter = SparkSubstraitConverter(self._options)
                substrait = self._converter.convert_plan(request.plan)
            case 'command':
                match request.plan.command.WhichOneof('command_type'):
                    case 'sql_command':
                        if (self._backend_with_tempview and
                                self._tempview_session_id == request.session_id):
                            substrait = convert_sql(request.plan.command.sql_command.sql,
                                                    self._backend_with_tempview)
                        else:
                            substrait = convert_sql(request.plan.command.sql_command.sql)
                    case 'create_dataframe_view':
                        if not self._converter and self._tempview_session_id != request.session_id:
                            self._converter = SparkSubstraitConverter(self._options)
                        self._backend_with_tempview = create_dataframe_view(
                            request.plan, self._options, self._backend_with_tempview)
                        self._tempview_session_id = request.session_id
                        self._converter.set_tempview_backend(self._backend_with_tempview)

                        return
                    case _:
                        type = request.plan.command.WhichOneof("command_type")
                        raise NotImplementedError(f'Unsupported command type: {type}')
            case _:
                raise ValueError(f'Unknown plan type: {request.plan}')
        _LOGGER.debug('  as Substrait: %s', substrait)
        if self._backend_with_tempview and self._tempview_session_id == request.session_id:
            backend = self._backend_with_tempview
        else:
            backend = find_backend(self._options.backend)
            backend.register_tpch()
        results = backend.execute(substrait)
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
        _LOGGER.info('AnalyzePlan: %s', request)
        if request.schema:
            if not self._converter:
                self._converter = SparkSubstraitConverter(self._options)
            substrait = self._converter.convert_plan(request.schema.plan)
            if self._backend_with_tempview and self._tempview_session_id == request.session_id:
                backend = self._backend_with_tempview
            else:
                backend = find_backend(self._options.backend)
                backend.register_tpch()
            results = backend.execute(substrait)
            _LOGGER.debug('  results are: %s', results)
            return pb2.AnalyzePlanResponse(
                session_id=request.session_id,
                schema=pb2.AnalyzePlanResponse.Schema(schema=convert_pyarrow_schema_to_spark(
                    results.schema)))
        raise NotImplementedError('AnalyzePlan not yet implemented for non-Schema requests.')

    def Config(self, request, context):
        """Get or set the configuration of the server."""
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
            case 'get_with_default':
                response.pairs.extend(request.operation.get_with_default.pairs)
        return response

    def AddArtifacts(self, request_iterator, context):
        """Add the given artifacts to the server."""
        _LOGGER.info('AddArtifacts')
        return pb2.AddArtifactsResponse()

    def ArtifactStatus(self, request, context):
        """Get the status of the given artifact."""
        _LOGGER.info('ArtifactStatus')
        return pb2.ArtifactStatusesResponse()

    def Interrupt(self, request, context):
        """Interrupt the execution of the given plan."""
        _LOGGER.info('Interrupt')
        return pb2.InterruptResponse()

    def ReattachExecute(
            self, request: pb2.ReattachExecuteRequest, context: grpc.RpcContext) -> Generator[
        pb2.ExecutePlanResponse, None, None]:
        """Reattach the execution of the given plan."""
        _LOGGER.info('ReattachExecute')
        yield pb2.ExecutePlanResponse(
            session_id=request.session_id,
            result_complete=pb2.ExecutePlanResponse.ResultComplete())

    def ReleaseExecute(self, request, context):
        """Release the execution of the given plan."""
        _LOGGER.info('ReleaseExecute')
        return pb2.ReleaseExecuteResponse()


def serve(port: int, wait: bool = True):
    """Start the SparkConnect to Substrait gateway server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_SparkConnectServiceServicer_to_server(SparkConnectService(), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    if wait:
        server.wait_for_termination()
        return None
    return server


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, encoding='utf-8')
    serve(50051)
