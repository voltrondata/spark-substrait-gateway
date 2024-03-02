# SPDX-License-Identifier: Apache-2.0
"""SparkConnect server that drives a backend using Substrait."""
from concurrent import futures
from typing import Generator

import grpc

import spark.connect.base_pb2_grpc as pb2_grpc
import spark.connect.base_pb2 as pb2
from gateway.converter.spark_to_substrait import SparkSubstraitConverter


# pylint: disable=E1101
class SparkConnectService(pb2_grpc.SparkConnectServiceServicer):
    """Provides the SparkConnect service."""

    def __init__(self, *args, **kwargs):
        pass

    def ExecutePlan(
            self, request: pb2.ExecutePlanRequest, context: grpc.RpcContext) -> Generator[
        pb2.ExecutePlanResponse, None, None]:
        print(f"ExecutePlan: {request}")
        convert = SparkSubstraitConverter()
        substrait = convert.convert_plan(request.plan)
        print(f"  as Substrait: {substrait}")
        yield pb2.ExecutePlanResponse(
            session_id=request.session_id,
            arrow_batch=pb2.ExecutePlanResponse.ArrowBatch(row_count=0, data=None))

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
        return pb2.ArtifictStatusResponse()

    def Interrupt(self, request, context):
        print("Interrupt")
        return pb2.InterruptResponse()

    def ReattachExecute(self, request, context):
        print("ReattachExecute")
        return pb2.ReattachExecuteResponse()

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
