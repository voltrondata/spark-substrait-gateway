# SPDX-License-Identifier: Apache-2.0
"""Routines to convert SparkConnect plans to Substrait plans."""
from substrait.gen.proto import plan_pb2
from substrait.gen.proto import algebra_pb2

import spark.connect.base_pb2 as spark_pb2
import spark.connect.expressions_pb2 as spark_expressions_pb2
import spark.connect.relations_pb2 as spark_relations_pb2


# pylint: disable=E1101,fixme
class SparkSubstraitConverter:
    """Converts SparkConnect plans to Substrait plans."""

    def convert_expression(self, expr: spark_expressions_pb2.Expression) -> algebra_pb2.Expression:
        """Converts a SparkConnect expression to a Substrait expression."""
        return algebra_pb2.Expression()

    def convert_read_named_table_relation(self, rel: spark_relations_pb2.Read) -> algebra_pb2.Rel:
        """Converts a read named table relation to a Substrait relation."""
        raise NotImplementedError('named tables are not yet implemented')

    def convert_read_data_source_relation(self, rel: spark_relations_pb2.Read) -> algebra_pb2.Rel:
        """Converts a read data source relation into a Substrait relation."""
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
        for p in rel.paths:
            local.items.append(algebra_pb2.ReadRel.LocalFiles.FileOrFiles(uri_file=p))
        return algebra_pb2.Rel(read=algebra_pb2.ReadRel(local_files=local))

    def convert_read_relation(self, rel: spark_relations_pb2.Read) -> algebra_pb2.Rel:
        """Converts a read relation into a Substrait relation."""
        match rel.WhichOneof('read_type'):
            case 'named_table':
                return self.convert_read_named_table_relation(rel.named_table)
            case 'data_source':
                return self.convert_read_data_source_relation(rel.data_source)
            case _:
                raise ValueError(f'Unexpected read type: {rel.WhichOneof("read_type")}')

    def convert_filter_relation(self, rel: spark_relations_pb2.Filter) -> algebra_pb2.Rel:
        """Converts a filter relation into a Substrait relation."""
        f = algebra_pb2.FilterRel(input=self.convert_relation(rel.input))
        f.condition.CopyFrom(self.convert_expression(rel.condition))
        return algebra_pb2.Rel(filter=f)

    def convert_sort_relation(self, rel: spark_relations_pb2.Sort) -> algebra_pb2.Rel:
        """Converts a sort relation into a Substrait relation."""
        sort = algebra_pb2.SortRel(input=self.convert_relation(rel.input))
        for order in rel.order:
            if order.direction == spark_expressions_pb2.Expression.SortOrder.SORT_DIRECTION_ASCENDING:
                if order.null_ordering == spark_expressions_pb2.Expression.SortOrder.SORT_NULLS_FIRST:
                    direction = algebra_pb2.SortField.SORT_DIRECTION_ASC_NULLS_FIRST
                else:
                    direction = algebra_pb2.SortField.SORT_DIRECTION_ASC_NULLS_LAST
            else:
                if order.null_ordering == spark_expressions_pb2.Expression.SortOrder.SORT_NULLS_FIRST:
                    direction = algebra_pb2.SortField.SORT_DIRECTION_DESC_NULLS_FIRST
                else:
                    direction = algebra_pb2.SortField.SORT_DIRECTION_DESC_NULLS_LAST
            sort.sorts.append(algebra_pb2.SortField(
                expr=self.convert_expression(order.child),
                direction=direction))
        return algebra_pb2.Rel(sort=sort)

    def convert_limit_relation(self, rel: spark_relations_pb2.Limit) -> algebra_pb2.Rel:
        """Converts a limit relation into a Substrait FetchRel relation."""
        return algebra_pb2.Rel(
            fetch=algebra_pb2.FetchRel(input=self.convert_relation(rel.input), count=rel.limit))

    def convert_aggregate_relation(self, rel: spark_relations_pb2.Aggregate) -> algebra_pb2.Rel:
        """Converts an aggregate relation into a Substrait relation."""
        # TODO -- Implement.
        return algebra_pb2.Rel(
            aggregate=algebra_pb2.AggregateRel(input=self.convert_relation(rel.input)))

    def convert_show_string_relation(self, rel: spark_relations_pb2.ShowString) -> algebra_pb2.Rel:
        """Converts a show string relation into a Substrait project relation."""
        # TODO -- Implement using num_rows, truncate, and vertical.
        return self.convert_relation(rel.input)

    def convert_with_columns_relation(
            self, rel: spark_relations_pb2.WithColumns) -> algebra_pb2.Rel:
        """Converts a with columns relation into a Substrait project relation."""
        project = algebra_pb2.ProjectRel(input=self.convert_relation(rel.input))
        num_emitted_fields = 0
        for alias in rel.aliases:
            # TODO -- Handle the output columns correctly.
            project.expressions.append(self.convert_expression(alias.expr))
            project.common.emit.output_mapping.append(num_emitted_fields)
        return algebra_pb2.Rel(project=project)

    # pylint: disable=too-many-return-statements
    def convert_relation(self, rel: spark_relations_pb2.Relation) -> algebra_pb2.Rel:
        """Converts a Spark relation into a Substrait one."""
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
        """Converts a Spark plan into a Substrait plan."""
        result = plan_pb2.Plan()
        if plan.HasField('root'):
            result.relations.append(plan_pb2.PlanRel(
                root=algebra_pb2.RelRoot(input=self.convert_relation(plan.root))))
        # TODO -- Add the extension_uris and extensions we referenced to result.
        return result
