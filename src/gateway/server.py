# SPDX-License-Identifier: Apache-2.0
"""SparkConnect server that drives a backend using Substrait."""
import io
from concurrent import futures
from typing import Generator

import grpc
import pyarrow
import pyspark.sql.connect.proto.base_pb2_grpc as pb2_grpc
import pyspark.sql.connect.proto.base_pb2 as pb2

from gateway.converter.conversion_options import duck_db
from gateway.converter.spark_to_substrait import SparkSubstraitConverter
from gateway.adbc.backend import AdbcBackend


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
        print(f"ExecutePlan: {request}")
        convert = SparkSubstraitConverter(self._options)
        substrait = convert.convert_plan(request.plan)
        print(f"  as Substrait: {substrait}")
        backend = AdbcBackend()
        results = backend.execute(substrait, self._options.backend)
        print(f"  results are: {results}")

        yield pb2.ExecutePlanResponse(
            session_id=request.session_id,
            arrow_batch=pb2.ExecutePlanResponse.ArrowBatch(row_count=results.num_rows,
                                                           data=show_string(results)))
        # TODO -- When spark 3.4.0 support is not required, yield a ResultComplete message here.

    def AnalyzePlan(self, request, context):
        print(f"AnalyzePlan: {request}")
        return pb2.AnalyzePlanResponse(session_id=request.session_id)

    def Config(self, request, context):
        print(f"Config: {request}")
        response = pb2.ConfigResponse(session_id=request.session_id)
        match request.operation.WhichOneof('op_type'):
            case 'set':
                response.pairs.extend(request.operation.set.pairs)
            case 'get_with_default':
                response.pairs.extend(request.operation.get_with_default.pairs)
        return response

    def AddArtifacts(self, request_iterator, context):
        print("AddArtifacts")
        return pb2.AddArtifactsResponse()

    def ArtifactStatus(self, request, context):
        print("ArtifactStatus")
        return pb2.ArtifactStatusesResponse()

    def Interrupt(self, request, context):
        print("Interrupt")
        return pb2.InterruptResponse()

    def ReattachExecute(self, request: pb2.ReattachExecuteRequest, context: grpc.RpcContext) -> \
    Generator[pb2.ExecutePlanResponse, None, None]:
        print("ReattachExecute")
        yield pb2.ExecutePlanResponse(
           session_id=request.session_id,
           result_complete=pb2.ExecutePlanResponse.ResultComplete())

    def ReleaseExecute(self, request, context):
        print("ReleaseExecute")
        return pb2.ReleaseExecuteResponse()


def serve():
    """Starts the SparkConnect to Substrait gateway server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_SparkConnectServiceServicer_to_server(SparkConnectService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
