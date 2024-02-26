import spark.connect.base_pb2 as spark_pb2
import spark.connect.relations_pb2 as spark_relations_pb2
import substrait.gen.proto.plan_pb2 as plan_pb2
import substrait.gen.proto.algebra_pb2 as algebra_pb2


class SparkSubstraitConverter:

    def convert_read_relation(self, rel: spark_relations_pb2.Read) -> algebra_pb2.Rel:
        return algebra_pb2.Rel(read=algebra_pb2.ReadRel())

    def convert_filter_relation(self, rel: spark_relations_pb2.Filter) -> algebra_pb2.Rel:
        return algebra_pb2.Rel(filter=algebra_pb2.FilterRel(input=self.convert_relation(rel.input)))

    def convert_sort_relation(self, rel: spark_relations_pb2.Sort) -> algebra_pb2.Rel:
        return algebra_pb2.Rel(sort=algebra_pb2.SortRel(input=self.convert_relation(rel.input)))

    def convert_limit_relation(self, rel: spark_relations_pb2.Limit) -> algebra_pb2.Rel:
        # TODO -- Implement.
        return self.convert_relation(rel.input)

    def convert_aggregate_relation(self, rel: spark_relations_pb2.Aggregate) -> algebra_pb2.Rel:
        return algebra_pb2.Rel(aggregate=algebra_pb2.AggregateRel(input=self.convert_relation(rel.input)))

    def convert_show_string_relation(self, rel: spark_relations_pb2.ShowString) -> algebra_pb2.Rel:
        # TODO -- Implement.
        return self.convert_relation(rel.input)

    def convert_with_columns_relation(self, rel: spark_relations_pb2.WithColumns) -> algebra_pb2.Rel:
        # TODO -- Implement.
        return self.convert_relation(rel.input)

    def convert_relation(self, rel: spark_relations_pb2.Relation) -> algebra_pb2.Rel:
        match rel.WhichOneof('rel_type'):
            case 'read':
                return self.convert_read_relation(rel.read)
            case 'filter':
                return self.convert_filter_relation(rel.filter)
            case 'sort':
                return self.convert_sort_relation(rel.sort)
            case 'limit':
                return self.convert_limit_relation(rel.limit)
            case 'aggregate':
                return self.convert_aggregate_relation(rel.aggregate)
            case 'show_string':
                return self.convert_show_string_relation(rel.show_string)
            case 'with_columns':
                return self.convert_with_columns_relation(rel.with_columns)
            case _:
                raise ValueError(f'Unexpected rel type: {rel.WhichOneof("rel_type")}')

    def convert_plan(self, plan: spark_pb2.Plan) -> plan_pb2.Plan:
        result = plan_pb2.Plan()
        if plan.HasField('root'):
            result.relations.append(plan_pb2.PlanRel(root=algebra_pb2.RelRoot(input=self.convert_relation(plan.root))))
        # TODO -- Add the extension_uris and extensions we referenced to result.
        return result
