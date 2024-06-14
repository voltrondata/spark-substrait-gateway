# SPDX-License-Identifier: Apache-2.0
import traceback
from contextlib import contextmanager

import google.protobuf.message
import pytest
import substrait_validator
from google.protobuf import json_format
from pyspark.errors.exceptions.connect import SparkConnectGrpcException
from substrait_validator.substrait import plan_pb2


def validate_plan(json_plan: str, ignore_too_few_names: bool):
    substrait_plan = json_format.Parse(json_plan, plan_pb2.Plan())
    try:
        config = substrait_validator.Config()
        if ignore_too_few_names:
            config.override_diagnostic_level(4003, "error", "info")
            config.override_diagnostic_level(1002, "error", "info")
        diagnostics = substrait_validator.plan_to_diagnostics(
            substrait_plan.SerializeToString(), config)
    except google.protobuf.message.DecodeError:
        # Probable protobuf mismatch internal to Substrait Validator, ignore for now.
        return
    issues = []
    for issue in diagnostics:
        if issue.adjusted_level >= substrait_validator.Diagnostic.LEVEL_ERROR:
            issues.append([issue.msg, substrait_validator.path_to_string(issue.path)])
    if issues:
        issues_as_text = '\n'.join(f'  → {issue[0]}\n    at {issue[1]}' for issue in issues)
        pytest.fail(f'Validation failed.  Issues:\n{issues_as_text}\n\nPlan:\n{substrait_plan}\n',
                    pytrace=False)


@contextmanager
def utilizes_valid_plans(session, caplog=None):
    """Validates that the plans used by the gateway backend pass validation."""
    if hasattr(session, 'sparkSession'):
        session = session.sparkSession
    # Reset the statistics, so we only see the plans that were created during our lifetime.
    if session.conf.get('spark-substrait-gateway.backend', 'spark') != 'spark':
        session.conf.set('spark-substrait-gateway.reset_statistics', None)
        ignore_too_few_names = session.conf.get(
            'spark-substrait-gateway.use_duckdb_struct_name_behavior') == 'True'
    else:
        ignore_too_few_names = False
    try:
        exception = None
        yield
    except SparkConnectGrpcException as e:
        exception = e
    if session.conf.get('spark-substrait-gateway.backend', 'spark') == 'spark':
        if exception:
            raise exception
        return
    plan_count = int(session.conf.get('spark-substrait-gateway.plan_count'))
    plans_as_text = []
    for i in range(plan_count):
        plan = session.conf.get(f'spark-substrait-gateway.plan.{i + 1}')
        plans_as_text.append(f'Plan #{i + 1}:\n{plan}\n')
        validate_plan(plan, ignore_too_few_names)
    if exception:
        log_text = "\n\n" + caplog.text if caplog else ""
        pytest.fail('Exception raised during execution: ' +
                    '\n'.join(traceback.format_exception(exception)) +
                    '\n\n'.join(plans_as_text)+log_text, pytrace=False)


