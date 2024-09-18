# SPDX-License-Identifier: Apache-2.0
"""Tests for the Spark to Substrait Gateway server."""

from pathlib import Path

import pytest

from gateway.tests.plan_validator import utilizes_valid_plans

test_case_directory = Path(__file__).resolve().parent / "data" / "tpc-ds"

sql_test_case_paths = [f for f in sorted(test_case_directory.iterdir()) if f.suffix == ".sql"]

sql_test_case_names = [p.stem for p in sql_test_case_paths]


@pytest.fixture(autouse=True)
def mark_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    source = request.getfixturevalue("source")
    path = request.getfixturevalue("path")
    if source == "gateway-over-duckdb":
        if path.stem in ["01", "06", "10", "16", "30", "32", "35", "69", "81", "86", "92", "94"]:
            pytest.skip(reason="DuckDB needs Delim join")
        elif path.stem in [
            "03",
            "04",
            "05",
            "07",
            "08",
            "11",
            "13",
            "14",
            "15",
            "17",
            "18",
            "19",
            "21",
            "22",
            "23",
            "25",
            "26",
            "27",
            "29",
            "31",
            "33",
            "34",
            "37",
            "39",
            "40",
            "42",
            "43",
            "45",
            "46",
            "48",
            "50",
            "52",
            "55",
            "56",
            "60",
            "61",
            "62",
            "64",
            "65",
            "66",
            "68",
            "71",
            "72",
            "73",
            "74",
            "75",
            "77",
            "78",
            "79",
            "80",
            "82",
            "85",
            "88",
            "90",
            "91",
            "96",
            "97",
            "99",
        ]:
            pytest.skip(reason="DuckDB INTERNAL Error: COMPARE_BETWEEN")
        elif path.stem in ["09"]:
            pytest.skip(
                reason="Binder Error: Cannot compare values of type VARCHAR and "
                "type INTEGER_LITERAL"
            )
        elif path.stem in [
            "12",
            "20",
            "36",
            "44",
            "47",
            "49",
            "51",
            "53",
            "57",
            "63",
            "67",
            "70",
            "89",
            "98",
        ]:
            pytest.skip(reason="DUCKDB INTERNAL Error: WINDOW")
        elif path.stem in ["24"]:
            pytest.skip(reason="INTERNAL Error: EMPTY_RESULT")
        elif path.stem in ["38", "41", "54", "87", "93"]:
            pytest.skip(reason="Error: Found unexpected child type in Distinct operator")
        elif path.stem in ["83"]:
            pytest.skip(reason="INTERNAL Error: Unsupported join type RIGHT_SEMI")
        elif path.stem in ["84"]:
            pytest.skip(reason="INTERNAL Error: COALESCE")
        elif path.stem in ["58"]:
            pytest.skip(reason="AssertionError: assert table is not None")
        elif path.stem in ["95"]:
            pytest.skip(reason="Unsupported join comparison: !=")
    if source == "gateway-over-datafusion":
        if path.stem in ["02"]:
            pytest.skip(reason="Null type without kind is not supported")
        elif path.stem in ["09"]:
            pytest.skip(reason="Aggregate function any_value is not supported: function anchor = 6")
        else:
            pytest.skip(reason="not yet ready to run SQL tests regularly")
    if source == "gateway-over-arrow":
        pytest.skip(reason="not yet ready to run SQL tests regularly")


class TestTpcds:
    @pytest.mark.timeout(60)
    @pytest.mark.parametrize(
        "path",
        sql_test_case_paths,
        ids=sql_test_case_names,
    )
    def test_tpcds(self, register_tpcds_dataset, spark_session, path, caplog):
        """Test the TPC-DS queries."""
        # Read the SQL to run.
        with open(path, "rb") as file:
            sql_bytes = file.read()
        sql = sql_bytes.decode("utf-8")

        with utilizes_valid_plans(spark_session, caplog):
            spark_session.sql(sql).collect()
