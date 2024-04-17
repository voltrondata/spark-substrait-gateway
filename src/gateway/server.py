# SPDX-License-Identifier: Apache-2.0
"""SparkConnect server that drives a backend using Substrait."""
import io
import logging
from concurrent import futures
from typing import Generator

import grpc
import pyarrow
import pyspark.sql.connect.proto.base_pb2 as pb2
import pyspark.sql.connect.proto.base_pb2_grpc as pb2_grpc
from pyspark.sql.connect.proto import types_pb2

from gateway.backends.backend_selector import find_backend
from gateway.converter.conversion_options import duck_db, datafusion
from gateway.converter.spark_to_substrait import SparkSubstraitConverter
from gateway.converter.sql_to_substrait import convert_sql

_LOGGER = logging.getLogger(__name__)


def show_string(table: pyarrow.lib.Table) -> bytes:
    """Converts a table into a byte serialized single row string column Arrow Table."""
    results_str = str(table)
    schema = pyarrow.schema([('show_string', pyarrow.string())])
    array = pyarrow.array([results_str])
    batch = pyarrow.RecordBatch.from_arrays([array], schema=schema)
    result_table = pyarrow.Table.from_batches([batch])
    buffer = io.BytesIO()
    stream = pyarrow.RecordBatchStreamWriter(buffer, schema)
    stream.write_table(result_table)
    stream.close()
    return buffer.getvalue()


def batch_to_bytes(batch: pyarrow.RecordBatch, schema: pyarrow.Schema) -> bytes:
    """Serializes a RecordBatch into a bytes."""
    result_table = pyarrow.Table.from_batches(batches=[batch])
    buffer = io.BytesIO()
    stream = pyarrow.RecordBatchStreamWriter(buffer, schema)
    stream.write_table(result_table)
    stream.close()
    return buffer.getvalue()


# pylint: disable=E1101
def convert_pyarrow_schema_to_spark(schema: pyarrow.Schema) -> types_pb2.DataType:
    """Converts a PyArrow schema to a SparkConnect DataType.Struct schema."""
    fields = []
    for field in schema:
        if field.type == pyarrow.bool_():
            data_type = types_pb2.DataType(boolean=types_pb2.DataType.Boolean())
        elif field.type == pyarrow.int8():
            data_type = types_pb2.DataType(byte=types_pb2.DataType.Byte())
        elif field.type == pyarrow.int16():
            data_type = types_pb2.DataType(integer=types_pb2.DataType.Short())
        elif field.type == pyarrow.int32():
            data_type = types_pb2.DataType(integer=types_pb2.DataType.Integer())
        elif field.type == pyarrow.int64():
            data_type = types_pb2.DataType(long=types_pb2.DataType.Long())
        elif field.type == pyarrow.float32():
            data_type = types_pb2.DataType(float=types_pb2.DataType.Float())
        elif field.type == pyarrow.float64():
            data_type = types_pb2.DataType(double=types_pb2.DataType.Double())
        elif field.type == pyarrow.string():
            data_type = types_pb2.DataType(string=types_pb2.DataType.String())
        elif field.type == pyarrow.timestamp('us'):
            data_type = types_pb2.DataType(timestamp=types_pb2.DataType.Timestamp())
        elif field.type == pyarrow.date32():
            data_type = types_pb2.DataType(date=types_pb2.DataType.Date())
        else:
            raise NotImplementedError(
                'Conversion from Arrow schema to Spark schema not yet implemented '
                f'for type: {field.type}')

        struct_field = types_pb2.DataType.StructField(name=field.name, data_type=data_type)
        fields.append(struct_field)

    return types_pb2.DataType(struct=types_pb2.DataType.Struct(fields=fields))


# pylint: disable=E1101,fixme
class SparkConnectService(pb2_grpc.SparkConnectServiceServicer):
    """Provides the SparkConnect service."""

    # pylint: disable=unused-argument
    def __init__(self, *args, **kwargs):
        # This is the central point for configuring the behavior of the service.
        self._options = duck_db()

    def ExecutePlan(
            self, request: pb2.ExecutePlanRequest, context: grpc.RpcContext) -> Generator[
        pb2.ExecutePlanResponse, None, None]:
        _LOGGER.info('ExecutePlan: %s', request)
        match request.plan.WhichOneof('op_type'):
            case 'root':
                convert = SparkSubstraitConverter(self._options)
                substrait = convert.convert_plan(request.plan)
            case 'command':
                match request.plan.command.WhichOneof('command_type'):
                    case 'sql_command':
                        substrait = convert_sql(request.plan.command.sql_command.sql)
                    case _:
                        raise NotImplementedError(
                            f'Unsupported command type: {request.plan.command.WhichOneof("command_type")}')
            case _:
                raise ValueError(f'Unknown plan type: {request.plan}')
        _LOGGER.debug('  as Substrait: %s', substrait)
        backend = find_backend(self._options.backend)
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
        _LOGGER.info('AnalyzePlan: %s', request)
        return pb2.AnalyzePlanResponse(session_id=request.session_id)

    def Config(self, request, context):
        _LOGGER.info('Config: %s', request)
        response = pb2.ConfigResponse(session_id=request.session_id)
        match request.operation.WhichOneof('op_type'):
            case 'set':
                for pair in request.operation.set.pairs:
                    if pair.key == 'spark-substrait-gateway.backend':
                        # Set the server backend for all connections (including ongoing ones).
                        match pair.value:
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
        _LOGGER.info('AddArtifacts')
        return pb2.AddArtifactsResponse()

    def ArtifactStatus(self, request, context):
        _LOGGER.info('ArtifactStatus')
        return pb2.ArtifactStatusesResponse()

    def Interrupt(self, request, context):
        _LOGGER.info('Interrupt')
        return pb2.InterruptResponse()

    def ReattachExecute(
            self, request: pb2.ReattachExecuteRequest, context: grpc.RpcContext) -> Generator[
        pb2.ExecutePlanResponse, None, None]:
        _LOGGER.info('ReattachExecute')
        yield pb2.ExecutePlanResponse(
            session_id=request.session_id,
            result_complete=pb2.ExecutePlanResponse.ResultComplete())

    def ReleaseExecute(self, request, context):
        _LOGGER.info('ReleaseExecute')
        return pb2.ReleaseExecuteResponse()


def serve(port: int, wait: bool = True):
    """Starts the SparkConnect to Substrait gateway server."""
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
