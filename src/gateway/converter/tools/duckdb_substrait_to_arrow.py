# SPDX-License-Identifier: Apache-2.0
"""Converts the provided plans from the DuckDB Substrait dialect to Acero's."""
import sys

from gateway.converter.label_relations import LabelRelations, UnlabelRelations
from gateway.converter.output_field_tracking_visitor import OutputFieldTrackingVisitor
from gateway.converter.simplify_casts import SimplifyCasts
from google.protobuf import json_format
from substrait.gen.proto import plan_pb2


# pylint: disable=E1101
def simplify_substrait_dialect(substrait_plan: plan_pb2.Plan) -> plan_pb2.Plan:
    """Translate a DuckDB dialect Substrait plan to an Arrow friendly one."""
    modified_plan = plan_pb2.Plan()
    modified_plan.CopyFrom(substrait_plan)
    # Add plan ids to every relation.
    LabelRelations().visit_plan(modified_plan)
    # Track the output fields.
    symbol_table = OutputFieldTrackingVisitor().visit_plan(modified_plan)
    # Replace all casts with projects of casts.
    SimplifyCasts(symbol_table).visit_plan(modified_plan)
    # Remove the plan id markers.
    UnlabelRelations().visit_plan(modified_plan)
    return modified_plan


# pylint: disable=E1101
# ruff: noqa: T201
def main():
    """Convert the provided plans from the DuckDB Substrait dialect to Acero's."""
    args = sys.argv[1:]
    if len(args) != 2:
        print("Usage: python duckdb_substrait_to_arrow.py <path to plan> <path to output plan>")
        sys.exit(1)

    with open(args[0], "rb") as file:
        plan_prototext = file.read()
    duckdb_plan = json_format.Parse(plan_prototext, plan_pb2.Plan())

    arrow_plan = simplify_substrait_dialect(duckdb_plan)

    with open(args[1], "w", encoding='utf-8') as file:
        file.write(json_format.MessageToJson(arrow_plan))


if __name__ == '__main__':
    main()
