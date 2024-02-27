import spark.connect.base_pb2 as spark_pb2
import spark.connect.relations_pb2 as spark_relations_pb2
import substrait.gen.proto.plan_pb2 as plan_pb2
import substrait.gen.proto.algebra_pb2 as algebra_pb2


class SparkSubstraitConverter:

    def convert_read_named_table_relation(self, rel: spark_relations_pb2.Read) -> algebra_pb2.Rel:
        raise NotImplementedError('named tables are not yet implemented')

    def convert_read_data_source_relation(self, rel: spark_relations_pb2.Read) -> algebra_pb2.Rel:
        local = algebra_pb2.ReadRel.LocalFiles()
        match rel.format:
            case 'parquest':
                local.parquet = algebra_pb2.ReadRel.ParquetReadOptions()
            case 'orc':
                local.parquet = algebra_pb2.ReadRel.OrcReadOptions()
            case 'text':
                raise NotImplementedError('the only supported formats are parquet and orc')
            case 'json':
                raise NotImplementedError('the only supported formats are parquet and orc')
            case 'csv':
                # TODO -- Implement CSV once Substrait has support.
                pass
            case 'avro':
               raise NotImplementedError('the only supported formats are parquet and orc')
            case 'arrow':
                local.parquet = algebra_pb2.ReadRel.ArrowReadOptions()
            case 'dwrf':
                local.parquet = algebra_pb2.ReadRel.DwrfReadOptions()
            case _:
                raise NotImplementedError(f'Unexpected file format: {rel.format}')
        # TODO -- Handle the schema.
        # options
        for p in rel.paths:
            local.items.append(algebra_pb2.ReadRel.LocalFiles.FileOrFiles(uri_file=p))
        return algebra_pb2.Rel(read=algebra_pb2.ReadRel(local_files=local))

    def convert_read_relation(self, rel: spark_relations_pb2.Read) -> algebra_pb2.Rel:
        match rel.WhichOneof('read_type'):
            case 'named_table':
                return self.convert_read_named_table_relation(rel.named_table)
            case 'data_source':
                return self.convert_read_data_source_relation(rel.data_source)
            case _:
                raise ValueError(f'Unexpected read type: {rel.WhichOneof("read_type")}')

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
