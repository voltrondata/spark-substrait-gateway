import spark.connect.base_pb2 as pb2
import substrait.gen.proto.plan_pb2 as plan_pb2


class SparkSubstraitConverter:

  def convert_plan(self, plan: pb2.Plan) -> plan_pb2.Plan:
    return plan_pb2.Plan()
