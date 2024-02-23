import grpc
from concurrent import futures
import time
import spark.connect.base_pb2_grpc as pb2_grpc
import spark.connect.base_pb2 as pb2


class SparkConnectService(pb2_grpc.SparkConnectServiceServicer):

    def __init__(self, *args, **kwargs):
        pass

    def ExecutePlan(self, request, context):
        print(f"ExecutePlan: {request}")

        # get the string from the incoming request
        #message = request.message
        #result = f'Hello I am up and running received "{message}" message from you'
        #result = {'message': result, 'received': True}

        yield pb2.ExecutePlanResponse(session_id=request.session_id)

    def AnalyzePlan(self, request, context):
        print("AnalyzePlan")
        return pb2.AnalyzePlanResponse()

    def Config(self, request, context):
        print("Config")
        return pb2.ConfigResponse()

    def AddArtifacts(self, request, context):
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
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_SparkConnectServiceServicer_to_server(SparkConnectService(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    serve()
