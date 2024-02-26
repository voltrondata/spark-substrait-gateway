import spark.connect.base_pb2 as spark_pb2
import spark.connect.relations_pb2 as spark_relations_pb2
import substrait.gen.proto.plan_pb2 as plan_pb2
import substrait.gen.proto.algebra_pb2 as algebra_pb2


class SparkSubstraitConverter:

    def convert_read_relation(self, rel: spark_relations_pb2.Read) -> algebra_pb2.Rel:
        return algebra_pb2.Rel()

    def convert_filter_relation(self, rel: spark_relations_pb2.Filter) -> algebra_pb2.Rel:
        return algebra_pb2.Rel()

    def convert_sort_relation(self, rel: spark_relations_pb2.Sort) -> algebra_pb2.Rel:
        return algebra_pb2.Rel()

    def convert_limit_relation(self, rel: spark_relations_pb2.Limit) -> algebra_pb2.Rel:
        return algebra_pb2.Rel()

    def convert_aggregate_relation(self, rel: spark_relations_pb2.Aggregate) -> algebra_pb2.Rel:
        return algebra_pb2.Rel()

    def convert_show_string_relation(self, rel: spark_relations_pb2.ShowString) -> algebra_pb2.Rel:
        return algebra_pb2.Rel()

    def convert_with_columns_relation(self, rel: spark_relations_pb2.WithColumns) -> algebra_pb2.Rel:
        return algebra_pb2.Rel()

    def convert_relation(self, rel: spark_relations_pb2.Relation) -> algebra_pb2.Rel:
        match rel.WhichOneof('rel_type'):
            case 'read':
                return self.convert_read_relation(rel)
            case 'filter':
                return self.convert_filter_relation(rel)
            case 'sort':
                return self.convert_sort_relation(rel)
            case 'limit':
                return self.convert_limit_relation(rel)
            case 'aggregate':
                return self.convert_aggregate_relation(rel)
            case 'show_string':
                return self.convert_show_string_relation(rel)
            case 'with_columns':
                return self.convert_with_columns_relation(rel)
            case _:
                raise ValueError(f'Unexpected rel type: {rel.WhichOneof("rel_type")}')

    def convert_plan(self, plan: spark_pb2.Plan) -> plan_pb2.Plan:
        result = plan_pb2.Plan()
        if plan.HasField('root'):
            self.convert_relation(plan.root)
        return result
