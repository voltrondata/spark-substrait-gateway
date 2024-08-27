# SPDX-License-Identifier: Apache-2.0
"""Tests for the Spark to Substrait Gateway server."""

from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
import pyspark
import pytest
from hamcrest import assert_that, equal_to
from pyspark import Row
from pyspark.errors.exceptions.connect import SparkConnectGrpcException
from pyspark.sql.functions import (
    bit_length,
    broadcast,
    btrim,
    char_length,
    character_length,
    coalesce,
    col,
    concat,
    concat_ws,
    contains,
    endswith,
    equal_null,
    expr,
    greatest,
    ifnull,
    instr,
    isnan,
    isnotnull,
    isnull,
    lcase,
    least,
    left,
    length,
    lit,
    locate,
    lower,
    lpad,
    ltrim,
    named_struct,
    nanvl,
    nullif,
    nvl,
    nvl2,
    octet_length,
    position,
    regexp,
    regexp_like,
    repeat,
    replace,
    right,
    rlike,
    row_number,
    rpad,
    rtrim,
    sqrt,
    startswith,
    substr,
    substring,
    trim,
    try_sum,
    ucase,
    upper,
)
from pyspark.sql.types import DoubleType, StructField, StructType
from pyspark.sql.window import Window
from pyspark.testing import assertDataFrameEqual

from gateway.tests.conftest import find_tpch
from gateway.tests.plan_validator import utilizes_valid_plans


def create_parquet_table(spark_session, table_name: str, table: pa.Table):
    """Creates a parquet table from a PyArrow table and registers it to the session."""
    pq.write_table(table, f"{table_name}.parquet")
    table_df = spark_session.read.parquet(f"{table_name}.parquet")
    table_df.createOrReplaceTempView(table_name)
    return spark_session.table(table_name)


@pytest.fixture(autouse=True)
def mark_dataframe_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    source = request.getfixturevalue("source")
    originalname = request.keywords.node.originalname
    if source == "gateway-over-datafusion":
        if originalname in ["test_column_getfield", "test_column_getitem"]:
            pytest.skip(reason="structs not handled")
    elif originalname == "test_column_getitem":
        pytest.skip(reason="maps and lists not handled")
    elif source == "spark" and originalname == "test_subquery_alias":
        pytest.xfail("Spark supports subquery_alias but everyone else does not")

    if source != "spark" and originalname.startswith("test_unionbyname"):
        pytest.skip(reason="unionByName not supported in Substrait")
    if source != "spark" and originalname.startswith("test_exceptall"):
        pytest.skip(reason="exceptAll not supported in Substrait")
    if source == "gateway-over-duckdb" and originalname in ["test_union", "test_unionall"]:
        pytest.skip(reason="DuckDB treats all unions as distinct")
    if source == "gateway-over-datafusion" and originalname == "test_subtract":
        pytest.skip(reason="subtract not supported")
    if source == "gateway-over-datafusion" and originalname == "test_intersect":
        pytest.skip(reason="intersect not supported")
    if source == "gateway-over-datafusion" and originalname == "test_offset":
        pytest.skip(reason="offset not supported")
    if source == "gateway-over-datafusion" and originalname == "test_broadcast":
        pytest.skip(reason="duplicate name problem with joins")
    if source == "gateway-over-duckdb" and originalname == "test_coalesce":
        pytest.skip(reason="missing Substrait mapping")
    if source == "gateway-over-datafusion" and originalname == "test_coalesce":
        pytest.skip(reason="datafusion cast error")
    if source == "spark" and originalname == "test_isnan":
        pytest.skip(reason="None not preserved")
    if source == "gateway-over-datafusion" and originalname in [
        "test_isnan",
        "test_nanvl",
        "test_least",
        "test_greatest",
    ]:
        pytest.skip(reason="missing Substrait mapping")
    if source != "spark" and originalname == "test_expr":
        pytest.skip(reason="SQL support needed in gateway")
    if source != "spark" and originalname == "test_named_struct":
        pytest.skip(reason="needs better type tracking in gateway")
    if source == "spark" and originalname == "test_nullif":
        pytest.skip(reason="internal Spark type error")
    if source == "gateway-over-duckdb" and originalname == "test_nullif":
        pytest.skip(reason="argument count issue in DuckDB mapping")
    if source != "spark" and originalname in ["test_locate", "test_position"]:
        pytest.skip(reason="no direct Substrait analog")
    if source == "gateway-over-duckdb" and originalname == "test_octet_length":
        pytest.skip(reason="varchar octet_length not supported")

    if source == "gateway-over-duckdb" and originalname == "test_sqrt":
        pytest.skip(reason="behavior option ignored")
    if source != "spark" and originalname in ["test_rint", "test_bround"]:
        pytest.skip(reason="behavior option ignored")
    if source != "spark" and originalname in ["test_negative", "test_negate", "test_positive"]:
        pytest.skip(reason="custom implementation required")
    if source == "gateway-over-duckdb" and originalname in [
        "test_acosh",
        "test_asinh",
        "test_atanh",
        "test_cosh",
        "test_sinh",
        "test_tanh",
    ]:
        pytest.skip(reason="missing implementation")
    if source == "gateway-over-datafusion" and originalname in ["test_sign", "test_signum"]:
        pytest.skip(reason="missing implementation")
    if source != "spark" and originalname in [
        "test_cot",
        "test_sec",
        "test_ln",
        "test_log",
        "test_log10",
        "test_log2",
        "test_log1p",
    ]:
        pytest.skip(reason="missing in Substrait")
    if source == "gateway-over-datafusion" and originalname == "test_try_divide":
        pytest.skip(reason="returns infinity instead of null")

    if source == "gateway-over-duckdb" and originalname == "test_row_number":
        pytest.skip(reason="window functions not yet implemented in DuckDB")


# ruff: noqa: E712
class TestDataFrameAPI:
    """Tests of the dataframe side of SparkConnect."""

    def test_collect(self, users_dataframe):
        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.collect()

        assert len(outcome) == 100

    # pylint: disable=singleton-comparison
    def test_filter(self, users_dataframe):
        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.filter(col("paid_for_service") == True).collect()

        assert len(outcome) == 29

    def test_create_dataframe(self, spark_session, caplog):
        expected = [
            Row(age=1, name="Alice"),
            Row(age=2, name="Bob"),
        ]

        with utilizes_valid_plans(spark_session, caplog):
            test_df = spark_session.createDataFrame([(1, "Alice"), (2, "Bob")], ["age", "name"])

        assertDataFrameEqual(test_df.collect(), expected)

    def test_create_dataframe_and_temp_view(self, spark_session, caplog):
        expected = [
            Row(age=1, name="Alice"),
            Row(age=2, name="Bob"),
        ]

        with utilizes_valid_plans(spark_session, caplog):
            test_df = spark_session.createDataFrame([(1, "Alice"), (2, "Bob")], ["age", "name"])
            test_df.createOrReplaceTempView("mytempview_from_df")
            view_df = spark_session.table("mytempview_from_df")

        assertDataFrameEqual(view_df.collect(), expected)

    def test_create_dataframe_then_join(self, register_tpch_dataset, spark_session, caplog):
        expected = [
            Row(c_custkey=131074, name="Alice", c_name="Customer#000131074"),
            Row(c_custkey=131075, name="Bob", c_name="Customer#000131075"),
        ]

        with utilizes_valid_plans(spark_session, caplog):
            customer_df = spark_session.table("customer")
            test_df = spark_session.createDataFrame(
                [(131074, "Alice"), (131075, "Bob")], ["c_custkey", "name"]
            )
            outcome = test_df.join(customer_df, on="c_custkey").collect()

        assertDataFrameEqual(outcome, expected)

    def test_dropna(self, spark_session, caplog):
        schema = pa.schema({"name": pa.string(), "age": pa.int32()})
        table = pa.Table.from_pydict(
            {"name": [None, "Joe", "Sarah", None], "age": [99, None, 42, None]}, schema=schema
        )
        test_df = create_parquet_table(spark_session, "mytesttable", table)

        with utilizes_valid_plans(test_df, caplog):
            outcome = test_df.dropna().collect()

        assert len(outcome) == 1

    def test_dropna_by_name(self, spark_session, caplog):
        schema = pa.schema({"name": pa.string(), "age": pa.int32()})
        table = pa.Table.from_pydict(
            {"name": [None, "Joe", "Sarah", None], "age": [99, None, 42, None]}, schema=schema
        )
        test_df = create_parquet_table(spark_session, "mytesttable", table)

        with utilizes_valid_plans(test_df, caplog):
            outcome = test_df.dropna(subset="name").collect()

        assert len(outcome) == 2

    def test_dropna_by_count(self, spark_session, caplog):
        schema = pa.schema({"name": pa.string(), "age": pa.int32()})
        table = pa.Table.from_pydict(
            {"name": [None, "Joe", "Sarah", None], "age": [99, None, 42, None]}, schema=schema
        )
        test_df = create_parquet_table(spark_session, "mytesttable", table)

        with utilizes_valid_plans(test_df, caplog):
            outcome = test_df.dropna(thresh=1).collect()

        assert len(outcome) == 3

    # pylint: disable=singleton-comparison
    def test_filter_with_show(self, users_dataframe, capsys):
        expected = """+-------------+---------------+----------------+
|      user_id|           name|paid_for_service|
+-------------+---------------+----------------+
|user669344115|   Joshua Brown|            true|
|user282427709|Michele Carroll|            true|
+-------------+---------------+----------------+

"""
        with utilizes_valid_plans(users_dataframe):
            users_dataframe.filter(col("paid_for_service") == True).limit(2).show()
            outcome = capsys.readouterr().out

        assert_that(outcome, equal_to(expected))

    # pylint: disable=singleton-comparison
    def test_filter_with_show_with_limit(self, users_dataframe, capsys):
        expected = """+-------------+------------+----------------+
|      user_id|        name|paid_for_service|
+-------------+------------+----------------+
|user669344115|Joshua Brown|            true|
+-------------+------------+----------------+
only showing top 1 row

"""
        with utilizes_valid_plans(users_dataframe):
            users_dataframe.filter(col("paid_for_service") == True).show(1)
            outcome = capsys.readouterr().out

        assert_that(outcome, equal_to(expected))

    # pylint: disable=singleton-comparison
    def test_filter_with_show_and_truncate(self, users_dataframe, capsys):
        expected = """+----------+----------+----------------+
|   user_id|      name|paid_for_service|
+----------+----------+----------------+
|user669...|Joshua ...|            true|
+----------+----------+----------------+

"""
        users_dataframe.filter(col("paid_for_service") == True).limit(1).show(truncate=10)
        outcome = capsys.readouterr().out
        assert_that(outcome, equal_to(expected))

    def test_count(self, users_dataframe):
        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.count()

        assert outcome == 100

    def test_limit(self, users_dataframe):
        expected = [
            Row(user_id="user849118289", name="Brooke Jones", paid_for_service=False),
            Row(user_id="user954079192", name="Collin Frank", paid_for_service=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.limit(2).collect()

        assertDataFrameEqual(outcome, expected)

    def test_with_column(self, users_dataframe):
        expected = [
            Row(user_id="user849118289", name="Brooke Jones", paid_for_service=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.withColumn("user_id", col("user_id")).limit(1).collect()

        assertDataFrameEqual(outcome, expected)
        assert list(outcome[0].asDict().keys()) == list(expected[0].asDict().keys())

    def test_with_column_changed(self, users_dataframe):
        expected = [
            Row(user_id="user849118289", name="Brooke", paid_for_service=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = (
                users_dataframe.withColumn("name", substring(col("name"), 1, 6)).limit(1).collect()
            )

        assertDataFrameEqual(outcome, expected)
        assert list(outcome[0].asDict().keys()) == list(expected[0].asDict().keys())

    def test_with_column_added(self, users_dataframe):
        expected = [
            Row(
                user_id="user849118289",
                name="Brooke Jones",
                paid_for_service=False,
                not_paid_for_service=True,
            ),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = (
                users_dataframe.withColumn(
                    "not_paid_for_service", ~users_dataframe.paid_for_service
                )
                .limit(1)
                .collect()
            )

        assertDataFrameEqual(outcome, expected)
        assert list(outcome[0].asDict().keys()) == list(expected[0].asDict().keys())

    def test_with_column_renamed(self, users_dataframe):
        expected = [
            Row(old_user_id="user849118289", name="Brooke Jones", paid_for_service=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.withColumnRenamed("user_id", "old_user_id").limit(1).collect()

        assertDataFrameEqual(outcome, expected)
        assert list(outcome[0].asDict().keys()) == list(expected[0].asDict().keys())

    def test_with_columns(self, users_dataframe):
        expected = [
            Row(
                user_id="user849118289",
                name="Brooke",
                paid_for_service=False,
                not_paid_for_service=True,
            ),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = (
                users_dataframe.withColumns(
                    {
                        "user_id": col("user_id"),
                        "name": substring(col("name"), 1, 6),
                        "not_paid_for_service": ~users_dataframe.paid_for_service,
                    }
                )
                .limit(1)
                .collect()
            )

        assertDataFrameEqual(outcome, expected)
        assert list(outcome[0].asDict().keys()) == list(expected[0].asDict().keys())

    def test_with_columns_renamed(self, users_dataframe):
        expected = [
            Row(old_user_id="user849118289", old_name="Brooke Jones", paid_for_service=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = (
                users_dataframe.withColumnsRenamed({"user_id": "old_user_id", "name": "old_name"})
                .limit(1)
                .collect()
            )

        assertDataFrameEqual(outcome, expected)
        assert list(outcome[0].asDict().keys()) == list(expected[0].asDict().keys())

    def test_drop(self, users_dataframe):
        expected = [
            Row(name="Brooke Jones", paid_for_service=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.drop(users_dataframe.user_id).limit(1).collect()

        assertDataFrameEqual(outcome, expected)
        assert list(outcome[0].asDict().keys()) == list(expected[0].asDict().keys())

    def test_drop_by_name(self, users_dataframe):
        expected = [
            Row(name="Brooke Jones", paid_for_service=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.drop("user_id").limit(1).collect()

        assertDataFrameEqual(outcome, expected)
        assert list(outcome[0].asDict().keys()) == list(expected[0].asDict().keys())

    def test_drop_all(self, users_dataframe, source):
        if source == "spark":
            outcome = (
                users_dataframe.drop("user_id")
                .drop("name")
                .drop("paid_for_service")
                .limit(1)
                .collect()
            )

            assert not outcome[0].asDict().keys()
        else:
            with pytest.raises(SparkConnectGrpcException):
                users_dataframe.drop("user_id").drop("name").drop("paid_for_service").limit(
                    1
                ).collect()

    def test_alias(self, users_dataframe):
        expected = [
            Row(foo="user849118289"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(col("user_id").alias("foo")).limit(1).collect()

        assertDataFrameEqual(outcome, expected)
        assert list(outcome[0].asDict().keys()) == ["foo"]

    def test_subquery_alias(self, users_dataframe):
        with pytest.raises(Exception) as exc_info:
            users_dataframe.select(col("user_id")).alias("foo").limit(1).collect()

        assert exc_info.match("Subquery alias relations are not yet implemented")

    def test_name(self, users_dataframe):
        expected = [
            Row(foo="user849118289"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(col("user_id").name("foo")).limit(1).collect()

        assertDataFrameEqual(outcome, expected)
        assert list(outcome[0].asDict().keys()) == ["foo"]

    def test_cast(self, users_dataframe):
        expected = [
            Row(user_id=849, name="Brooke Jones", paid_for_service=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = (
                users_dataframe.withColumn(
                    "user_id", substring(col("user_id"), 5, 3).cast("integer")
                )
                .limit(1)
                .collect()
            )

        assertDataFrameEqual(outcome, expected)

    def test_getattr(self, users_dataframe):
        expected = [
            Row(user_id="user669344115"),
            Row(user_id="user849118289"),
            Row(user_id="user954079192"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(users_dataframe.user_id).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_getitem(self, users_dataframe):
        expected = [
            Row(user_id="user669344115"),
            Row(user_id="user849118289"),
            Row(user_id="user954079192"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(users_dataframe["user_id"]).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_column_getfield(self, spark_session, caplog):
        expected = [
            Row(answer="b", answer2=1),
        ]

        struct_type = pa.struct([("a", pa.int64()), ("b", pa.string())])
        data = [{"a": 1, "b": "b"}]
        struct_array = pa.array(data, type=struct_type)
        table = pa.Table.from_arrays([struct_array], names=["r"])

        df = create_parquet_table(spark_session, "mytesttable", table)

        with utilizes_valid_plans(df):
            outcome = df.select(df.r.getField("b"), df.r.a).collect()

        assertDataFrameEqual(outcome, expected)

    def test_column_getitem(self, spark_session):
        expected = [
            Row(answer=1, answer2="value"),
        ]

        list_array = pa.array([[1, 2]], type=pa.list_(pa.int64()))
        map_array = pa.array([{"key": "value"}], type=pa.map_(pa.string(), pa.string(), False))
        table = pa.Table.from_arrays([list_array, map_array], names=["l", "d"])

        df = create_parquet_table(spark_session, "mytesttable", table)

        with utilizes_valid_plans(df):
            outcome = df.select(df.l.getItem(0), df.d.getItem("key")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_astype(self, users_dataframe):
        expected = [
            Row(user_id=849, name="Brooke Jones", paid_for_service=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = (
                users_dataframe.withColumn(
                    "user_id", substring(col("user_id"), 5, 3).astype("integer")
                )
                .limit(1)
                .collect()
            )

        assertDataFrameEqual(outcome, expected)

    def test_join(self, register_tpch_dataset, spark_session):
        expected = [
            Row(
                n_nationkey=5,
                n_name="ETHIOPIA",
                n_regionkey=0,
                n_comment="regular requests sleep carefull",
                s_suppkey=2,
                s_name="Supplier#000000002",
                s_address="TRMhVHz3XiFuhapxucPo1",
                s_nationkey=5,
                s_phone="15-679-861-2259",
                s_acctbal=Decimal("4032.68"),
                s_comment=" the pending packages. furiously expres",
            ),
        ]

        with utilizes_valid_plans(spark_session):
            nation = spark_session.table("nation")
            supplier = spark_session.table("supplier")

            nat = nation.join(supplier, col("n_nationkey") == col("s_nationkey"))
            outcome = nat.filter(col("s_suppkey") == 2).limit(1).collect()

        assertDataFrameEqual(outcome, expected)

    def test_crossjoin(self, register_tpch_dataset, spark_session):
        expected = [
            Row(n_nationkey=0, n_name="ALGERIA", s_name="Supplier#000000002"),
            Row(n_nationkey=1, n_name="ARGENTINA", s_name="Supplier#000000002"),
            Row(n_nationkey=2, n_name="BRAZIL", s_name="Supplier#000000002"),
            Row(n_nationkey=3, n_name="CANADA", s_name="Supplier#000000002"),
            Row(n_nationkey=4, n_name="EGYPT", s_name="Supplier#000000002"),
        ]

        with utilizes_valid_plans(spark_session):
            nation = spark_session.table("nation")
            supplier = spark_session.table("supplier")

            nat = nation.crossJoin(supplier).filter(col("s_suppkey") == 2)
            outcome = nat.select("n_nationkey", "n_name", "s_name").limit(5).collect()

        assertDataFrameEqual(outcome, expected)

    def test_union(self, register_tpch_dataset, spark_session):
        expected = [
            Row(
                n_nationkey=23,
                n_name="UNITED KINGDOM",
                n_regionkey=3,
                n_comment="carefully pending courts sleep above the ironic, regular theo",
            ),
            Row(
                n_nationkey=23,
                n_name="UNITED KINGDOM",
                n_regionkey=3,
                n_comment="carefully pending courts sleep above the ironic, regular theo",
            ),
        ]

        with utilizes_valid_plans(spark_session):
            nation = spark_session.table("nation")

            outcome = nation.union(nation).filter(col("n_nationkey") == 23).collect()

        assertDataFrameEqual(outcome, expected)

    def test_union_distinct(self, register_tpch_dataset, spark_session):
        expected = [
            Row(
                n_nationkey=23,
                n_name="UNITED KINGDOM",
                n_regionkey=3,
                n_comment="carefully pending courts sleep above the ironic, regular theo",
            ),
        ]

        with utilizes_valid_plans(spark_session):
            nation = spark_session.table("nation")

            outcome = nation.union(nation).distinct().filter(col("n_nationkey") == 23).collect()

        assertDataFrameEqual(outcome, expected)

    def test_unionall(self, register_tpch_dataset, spark_session):
        expected = [
            Row(
                n_nationkey=23,
                n_name="UNITED KINGDOM",
                n_regionkey=3,
                n_comment="carefully pending courts sleep above the ironic, regular theo",
            ),
            Row(
                n_nationkey=23,
                n_name="UNITED KINGDOM",
                n_regionkey=3,
                n_comment="carefully pending courts sleep above the ironic, regular theo",
            ),
        ]

        with utilizes_valid_plans(spark_session):
            nation = spark_session.table("nation")

            outcome = nation.unionAll(nation).filter(col("n_nationkey") == 23).collect()

        assertDataFrameEqual(outcome, expected)

    def test_exceptall(self, register_tpch_dataset, spark_session, caplog):
        expected = [
            Row(
                n_nationkey=21,
                n_name="VIETNAM",
                n_regionkey=2,
                n_comment="lly across the quickly even pinto beans. caref",
            ),
            Row(
                n_nationkey=21,
                n_name="VIETNAM",
                n_regionkey=2,
                n_comment="lly across the quickly even pinto beans. caref",
            ),
            Row(
                n_nationkey=22,
                n_name="RUSSIA",
                n_regionkey=3,
                n_comment="uctions. furiously unusual instructions sleep furiously ironic "
                "packages. slyly ",
            ),
            Row(
                n_nationkey=22,
                n_name="RUSSIA",
                n_regionkey=3,
                n_comment="uctions. furiously unusual instructions sleep furiously ironic "
                "packages. slyly ",
            ),
            Row(
                n_nationkey=23,
                n_name="UNITED KINGDOM",
                n_regionkey=3,
                n_comment="carefully pending courts sleep above the ironic, regular theo",
            ),
            Row(
                n_nationkey=24,
                n_name="UNITED STATES",
                n_regionkey=1,
                n_comment="ly ironic requests along the slyly bold ideas hang after the "
                "blithely special notornis; blithely even accounts",
            ),
            Row(
                n_nationkey=24,
                n_name="UNITED STATES",
                n_regionkey=1,
                n_comment="ly ironic requests along the slyly bold ideas hang after the "
                "blithely special notornis; blithely even accounts",
            ),
        ]

        with utilizes_valid_plans(spark_session, caplog):
            nation = spark_session.table("nation").filter(col("n_nationkey") > 20)
            nation1 = nation.union(nation)
            nation2 = nation.filter(col("n_nationkey") == 23)

            outcome = nation1.exceptAll(nation2).collect()

        assertDataFrameEqual(outcome, expected)

    def test_distinct(self, spark_session):
        expected = [
            Row(a=1, b=10, c="a"),
            Row(a=2, b=11, c="a"),
            Row(a=3, b=12, c="a"),
            Row(a=4, b=13, c="a"),
        ]

        int1_array = pa.array([1, 2, 3, 3, 4], type=pa.int32())
        int2_array = pa.array([10, 11, 12, 12, 13], type=pa.int32())
        string_array = pa.array(["a", "a", "a", "a", "a"], type=pa.string())
        table = pa.Table.from_arrays([int1_array, int2_array, string_array], names=["a", "b", "c"])

        df = create_parquet_table(spark_session, "mytesttable1", table)

        with utilizes_valid_plans(df):
            outcome = df.distinct().collect()

        assertDataFrameEqual(outcome, expected)

    def test_drop_duplicates(self, spark_session):
        expected = [
            Row(a=1, b=10, c="a"),
            Row(a=2, b=11, c="a"),
            Row(a=3, b=12, c="a"),
            Row(a=4, b=13, c="a"),
        ]

        int1_array = pa.array([1, 2, 3, 3, 4], type=pa.int32())
        int2_array = pa.array([10, 11, 12, 12, 13], type=pa.int32())
        string_array = pa.array(["a", "a", "a", "a", "a"], type=pa.string())
        table = pa.Table.from_arrays([int1_array, int2_array, string_array], names=["a", "b", "c"])

        df = create_parquet_table(spark_session, "mytesttable2", table)

        with utilizes_valid_plans(df):
            outcome = df.dropDuplicates().collect()

        assertDataFrameEqual(outcome, expected)

    def test_colregex(self, spark_session, caplog):
        expected = [
            Row(a1=1, col2="a"),
            Row(a1=2, col2="b"),
            Row(a1=3, col2="c"),
        ]

        int_array = pa.array([1, 2, 3], type=pa.int32())
        string_array = pa.array(["a", "b", "c"], type=pa.string())
        table = pa.Table.from_arrays(
            [int_array, int_array, string_array, string_array], names=["a1", "c", "col", "col2"]
        )

        df = create_parquet_table(spark_session, "mytesttable3", table)

        with utilizes_valid_plans(df, caplog):
            outcome = df.select(df.colRegex("`(c.l|a)?[0-9]`")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_subtract(self, register_tpch_dataset, spark_session):
        expected = [
            Row(
                n_nationkey=21,
                n_name="VIETNAM",
                n_regionkey=2,
                n_comment="lly across the quickly even pinto beans. caref",
            ),
            Row(
                n_nationkey=22,
                n_name="RUSSIA",
                n_regionkey=3,
                n_comment="uctions. furiously unusual instructions sleep furiously "
                "ironic packages. slyly ",
            ),
            Row(
                n_nationkey=24,
                n_name="UNITED STATES",
                n_regionkey=1,
                n_comment="ly ironic requests along the slyly bold ideas hang after "
                "the blithely special notornis; blithely even accounts",
            ),
        ]

        with utilizes_valid_plans(spark_session):
            nation = spark_session.table("nation").filter(col("n_nationkey") > 20)
            nation1 = nation.union(nation)
            nation2 = nation.filter(col("n_nationkey") == 23)

            outcome = nation1.subtract(nation2).collect()

        assertDataFrameEqual(outcome, expected)

    def test_intersect(self, register_tpch_dataset, spark_session):
        expected = [
            Row(
                n_nationkey=23,
                n_name="UNITED KINGDOM",
                n_regionkey=3,
                n_comment="carefully pending courts sleep above the ironic, regular theo",
            ),
        ]

        with utilizes_valid_plans(spark_session):
            nation = spark_session.table("nation")
            nation1 = nation.union(nation).filter(col("n_nationkey") >= 23)
            nation2 = nation.filter(col("n_nationkey") <= 23)

            outcome = nation1.intersect(nation2).collect()

        assertDataFrameEqual(outcome, expected)

    def test_unionbyname(self, spark_session):
        expected = [
            Row(a=1, b=2, c=3),
            Row(a=4, b=5, c=6),
        ]

        int1_array = pa.array([1], type=pa.int32())
        int2_array = pa.array([2], type=pa.int32())
        int3_array = pa.array([3], type=pa.int32())
        table = pa.Table.from_arrays([int1_array, int2_array, int3_array], names=["a", "b", "c"])
        int4_array = pa.array([4], type=pa.int32())
        int5_array = pa.array([5], type=pa.int32())
        int6_array = pa.array([6], type=pa.int32())
        table2 = pa.Table.from_arrays([int4_array, int5_array, int6_array], names=["a", "b", "c"])

        df = create_parquet_table(spark_session, "mytesttable1", table)
        df2 = create_parquet_table(spark_session, "mytesttable2", table2)

        with utilizes_valid_plans(df):
            outcome = df.unionByName(df2).collect()
            assertDataFrameEqual(outcome, expected)

    def test_unionbyname_with_mismatched_columns(self, spark_session):
        int1_array = pa.array([1], type=pa.int32())
        int2_array = pa.array([2], type=pa.int32())
        int3_array = pa.array([3], type=pa.int32())
        table = pa.Table.from_arrays([int1_array, int2_array, int3_array], names=["a", "b", "c"])
        int4_array = pa.array([4], type=pa.int32())
        int5_array = pa.array([5], type=pa.int32())
        int6_array = pa.array([6], type=pa.int32())
        table2 = pa.Table.from_arrays([int4_array, int5_array, int6_array], names=["b", "c", "d"])

        df = create_parquet_table(spark_session, "mytesttable1", table)
        df2 = create_parquet_table(spark_session, "mytesttable2", table2)

        with pytest.raises(pyspark.errors.exceptions.captured.AnalysisException):
            df.unionByName(df2).collect()

    def test_unionbyname_with_missing_columns(self, spark_session):
        expected = [
            Row(a=1, b=2, c=3, d=None),
            Row(a=None, b=4, c=5, d=6),
        ]

        int1_array = pa.array([1], type=pa.int32())
        int2_array = pa.array([2], type=pa.int32())
        int3_array = pa.array([3], type=pa.int32())
        table = pa.Table.from_arrays([int1_array, int2_array, int3_array], names=["a", "b", "c"])
        int4_array = pa.array([4], type=pa.int32())
        int5_array = pa.array([5], type=pa.int32())
        int6_array = pa.array([6], type=pa.int32())
        table2 = pa.Table.from_arrays([int4_array, int5_array, int6_array], names=["b", "c", "d"])

        df = create_parquet_table(spark_session, "mytesttable1", table)
        df2 = create_parquet_table(spark_session, "mytesttable2", table2)

        with utilizes_valid_plans(df):
            outcome = df.unionByName(df2, allowMissingColumns=True).collect()
            assertDataFrameEqual(outcome, expected)

    def test_between(self, register_tpch_dataset, spark_session):
        expected = [
            Row(n_name="ALGERIA", is_between=False),
            Row(n_name="ARGENTINA", is_between=False),
            Row(n_name="BRAZIL", is_between=False),
            Row(n_name="CANADA", is_between=False),
            Row(n_name="EGYPT", is_between=True),
        ]

        with utilizes_valid_plans(spark_session):
            nation = spark_session.table("nation")

            outcome = (
                nation.select(nation.n_name, nation.n_regionkey.between(2, 4).name("is_between"))
                .limit(5)
                .collect()
            )

            assertDataFrameEqual(outcome, expected)

    def test_eqnullsafe(self, spark_session):
        expected = [
            Row(a=None, b=False, c=True),
            Row(a=True, b=True, c=False),
        ]
        expected2 = [
            Row(a=False, b=False, c=True),
            Row(a=False, b=True, c=False),
            Row(a=True, b=False, c=False),
        ]

        string_array = pa.array(["foo", None, None], type=pa.string())
        float_array = pa.array([float("NaN"), 42.0, None], type=pa.float64())
        table = pa.Table.from_arrays([string_array, float_array], names=["s", "f"])

        df = create_parquet_table(spark_session, "mytesttable", table)

        with utilizes_valid_plans(df):
            outcome = (
                df.select(df.s == "foo", df.s.eqNullSafe("foo"), df.s.eqNullSafe(None))
                .limit(2)
                .collect()
            )
            assertDataFrameEqual(outcome, expected)

            outcome = df.select(
                df.f.eqNullSafe(None), df.f.eqNullSafe(float("NaN")), df.f.eqNullSafe(42.0)
            ).collect()
            assertDataFrameEqual(outcome, expected2)

    def test_bitwise(self, spark_session):
        expected = [
            Row(a=0, b=42, c=42),
            Row(a=42, b=42, c=0),
            Row(a=8, b=255, c=247),
            Row(a=None, b=None, c=None),
        ]

        int_array = pa.array([221, 0, 42, None], type=pa.int64())
        table = pa.Table.from_arrays([int_array], names=["i"])

        df = create_parquet_table(spark_session, "mytesttable", table)

        with utilizes_valid_plans(df):
            outcome = df.select(
                df.i.bitwiseAND(42), df.i.bitwiseOR(42), df.i.bitwiseXOR(42)
            ).collect()
            assertDataFrameEqual(outcome, expected)

    def test_first(self, users_dataframe):
        expected = Row(user_id="user012015386", name="Kelly Mcdonald", paid_for_service=False)

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.sort("user_id").first()

        assert outcome == expected

    def test_head(self, users_dataframe):
        expected = [
            Row(user_id="user012015386", name="Kelly Mcdonald", paid_for_service=False),
            Row(user_id="user041132632", name="Tina Atkinson", paid_for_service=False),
            Row(user_id="user056872864", name="Kenneth Castro", paid_for_service=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.sort("user_id").head(3)
            assertDataFrameEqual(outcome, expected)

    def test_take(self, users_dataframe):
        expected = [
            Row(user_id="user012015386", name="Kelly Mcdonald", paid_for_service=False),
            Row(user_id="user041132632", name="Tina Atkinson", paid_for_service=False),
            Row(user_id="user056872864", name="Kenneth Castro", paid_for_service=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.sort("user_id").take(3)
            assertDataFrameEqual(outcome, expected)

    def test_offset(self, users_dataframe):
        expected = [
            Row(user_id="user056872864", name="Kenneth Castro", paid_for_service=False),
            Row(user_id="user058232666", name="Collin Goodwin", paid_for_service=False),
            Row(user_id="user065694278", name="Rachel Mclean", paid_for_service=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.sort("user_id").offset(2).head(3)
            assertDataFrameEqual(outcome, expected)

    def test_tail(self, users_dataframe, source):
        expected = [
            Row(user_id="user990459354", name="Kevin Hall", paid_for_service=False),
            Row(user_id="user995187670", name="Rebecca Valentine", paid_for_service=False),
            Row(user_id="user995208610", name="Helen Clark", paid_for_service=False),
        ]

        if source == "spark":
            outcome = users_dataframe.sort("user_id").tail(3)
            assertDataFrameEqual(outcome, expected)
        else:
            with pytest.raises(SparkConnectGrpcException):
                users_dataframe.sort("user_id").tail(3)

    @pytest.mark.skip(reason="Not implemented by Spark Connect")
    def test_foreach(self, users_dataframe):
        def noop(_):
            pass

        with utilizes_valid_plans(users_dataframe):
            users_dataframe.limit(3).foreach(noop)

    @pytest.mark.skip(reason="Spark Connect throws an exception on empty tables")
    def test_isempty(self, users_dataframe, spark_session, caplog):
        with utilizes_valid_plans(users_dataframe, caplog):
            outcome = users_dataframe.limit(3)
            assert not outcome.isEmpty()

        with utilizes_valid_plans(users_dataframe, caplog):
            outcome = users_dataframe.limit(0)
            assert outcome.isEmpty()

        with utilizes_valid_plans(users_dataframe, caplog):
            empty = spark_session.createDataFrame([], users_dataframe.schema)
            assert empty.isEmpty()

    def test_select_expr(self, users_dataframe, source, caplog):
        expected = [
            Row(user_id="849118289", b=True),
        ]

        if source == "spark":
            with utilizes_valid_plans(users_dataframe, caplog):
                outcome = (
                    users_dataframe.selectExpr("substr(user_id, 5, 9)", "not paid_for_service")
                    .limit(1)
                    .collect()
                )

            assertDataFrameEqual(outcome, expected)
        else:
            with pytest.raises(SparkConnectGrpcException):
                users_dataframe.selectExpr("substr(user_id, 5, 9)", "not paid_for_service").limit(
                    1
                ).collect()

    def test_data_source_schema(self, spark_session):
        location_customer = str(find_tpch() / "customer.parquet")
        schema = spark_session.read.parquet(location_customer).schema
        assert len(schema) == 8

    def test_data_source_filter(self, spark_session):
        location_customer = str(find_tpch() / "customer.parquet")
        customer_dataframe = spark_session.read.parquet(location_customer)

        with utilizes_valid_plans(spark_session):
            outcome = customer_dataframe.filter(col("c_mktsegment") == "FURNITURE").collect()

        assert len(outcome) == 29968

    def test_data_source_options(self, spark_session):
        location_customer = str(find_tpch() / "customer.parquet")
        spark_options = {"path": location_customer}
        customer_dataframe = spark_session.read.format("parquet").options(**spark_options).load()

        with utilizes_valid_plans(spark_session):
            outcome = customer_dataframe.filter(col("c_mktsegment") == "FURNITURE").collect()

        assert len(outcome) == 29968

    def test_table(self, register_tpch_dataset, spark_session):
        with utilizes_valid_plans(spark_session):
            outcome = spark_session.table("customer").collect()

        assert len(outcome) == 150000

    def test_table_schema(self, register_tpch_dataset, spark_session):
        schema = spark_session.table("customer").schema
        assert len(schema) == 8

    def test_table_filter(self, register_tpch_dataset, spark_session):
        customer_dataframe = spark_session.table("customer")

        with utilizes_valid_plans(spark_session):
            outcome = customer_dataframe.filter(col("c_mktsegment") == "FURNITURE").collect()

        assert len(outcome) == 29968

    def test_create_or_replace_temp_view(self, spark_session):
        location_customer = str(find_tpch() / "customer.parquet")
        df_customer = spark_session.read.parquet(location_customer)
        df_customer.createOrReplaceTempView("mytempview")

        with utilizes_valid_plans(spark_session):
            outcome = spark_session.table("mytempview").collect()

        assert len(outcome) == 150000

    def test_create_or_replace_multiple_temp_views(self, spark_session):
        location_customer = str(find_tpch() / "customer.parquet")
        df_customer = spark_session.read.parquet(location_customer)
        df_customer.createOrReplaceTempView("mytempview1")
        df_customer.createOrReplaceTempView("mytempview2")

        with utilizes_valid_plans(spark_session):
            outcome1 = spark_session.table("mytempview1").collect()
            outcome2 = spark_session.table("mytempview2").collect()

        assert len(outcome1) == len(outcome2) == 150000


class TestDataFrameAPIFunctions:
    """Tests functions of the dataframe side of SparkConnect."""

    def test_lit(self, users_dataframe):
        expected = [
            Row(a=42),
            Row(a=42),
            Row(a=42),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(lit(42)).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_broadcast(self, users_dataframe):
        expected = [
            Row(
                user_id="user849118289",
                name="Brooke Jones",
                paid_for_service=False,
                revised_user_id="user849118289",
                short_name="Brooke",
            ),
        ]

        with utilizes_valid_plans(users_dataframe):
            revised_users_df = users_dataframe.withColumns(
                {
                    "revised_user_id": col("user_id"),
                    "short_name": substring(col("name"), 1, 6),
                }
            ).drop("user_id", "name", "paid_for_service")
            outcome = (
                users_dataframe.join(
                    broadcast(revised_users_df),
                    revised_users_df.revised_user_id == users_dataframe.user_id,
                )
                .limit(1)
                .collect()
            )

        assertDataFrameEqual(outcome, expected)

    def test_coalesce(self, spark_session):
        expected = [
            Row(a="42.0"),
            Row(a="foo"),
            Row(a=None),
        ]

        string_array = pa.array(["foo", None, None], type=pa.string())
        float_array = pa.array([float("NaN"), 42.0, None], type=pa.float64())
        table = pa.Table.from_arrays([string_array, float_array], names=["s", "f"])

        df = create_parquet_table(spark_session, "mytesttable", table)

        with utilizes_valid_plans(df):
            outcome = df.select(coalesce("s", "f")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_isnull(self, spark_session):
        expected = [
            Row(a=False, b=False),
            Row(a=True, b=False),
            Row(a=True, b=True),
        ]

        string_array = pa.array(["foo", None, None], type=pa.string())
        float_array = pa.array([float("NaN"), 42.0, None], type=pa.float64())
        table = pa.Table.from_arrays([string_array, float_array], names=["s", "f"])

        df = create_parquet_table(spark_session, "mytesttable", table)

        with utilizes_valid_plans(df):
            outcome = df.select(isnull("s"), isnull("f")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_isnan(self, spark_session):
        expected = [
            Row(f=42.0, a=False),
            Row(f=None, a=None),
            Row(f=float("NaN"), a=True),
        ]

        float_array = pa.array([float("NaN"), 42.0, None], type=pa.float64())
        table = pa.Table.from_arrays([float_array], names=["f"])

        df = create_parquet_table(spark_session, "mytesttable", table)

        with utilizes_valid_plans(df):
            outcome = df.select(col("f"), isnan("f")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_nanvl(self, spark_session):
        expected = [
            Row(a=42.0),
            Row(a=9999.0),
            Row(a=None),
        ]

        float_array = pa.array([float("NaN"), 42.0, None], type=pa.float64())
        table = pa.Table.from_arrays([float_array], names=["f"])

        df = create_parquet_table(spark_session, "mytesttable", table)

        with utilizes_valid_plans(df):
            outcome = df.select(nanvl("f", lit(float(9999)))).collect()

        assertDataFrameEqual(outcome, expected)

    def test_expr(self, users_dataframe):
        expected = [
            Row(name="Brooke Jones", a=12),
            Row(name="Collin Frank", a=12),
            Row(name="Joshua Brown", a=12),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select("name", expr("length(name)")).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_least(self, spark_session):
        expected = [
            Row(a=-12.0),
            Row(a=42.0),
            Row(a=63.0),
            Row(a=None),
        ]

        float1_array = pa.array([63, 42.0, None, 12], type=pa.float64())
        float2_array = pa.array([float("NaN"), 42.0, None, -12], type=pa.float64())
        table = pa.Table.from_arrays([float1_array, float2_array], names=["f1", "f2"])

        df = create_parquet_table(spark_session, "mytesttable", table)

        with utilizes_valid_plans(df):
            outcome = df.select(least("f1", "f2")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_greatest(self, spark_session):
        expected = [
            Row(a=12.0),
            Row(a=42.0),
            Row(a=None),
            Row(a=float("NaN")),
        ]

        float1_array = pa.array([63, 42.0, None, 12], type=pa.float64())
        float2_array = pa.array([float("NaN"), 42.0, None, -12], type=pa.float64())
        table = pa.Table.from_arrays([float1_array, float2_array], names=["f1", "f2"])

        df = create_parquet_table(spark_session, "mytesttable", table)

        with utilizes_valid_plans(df):
            outcome = df.select(greatest("f1", "f2")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_named_struct(self, spark_session):
        expected = [
            Row(named_struct=Row(a="bar", b=2)),
            Row(named_struct=Row(a="foo", b=1)),
        ]

        int_array = pa.array([1, 2], type=pa.int32())
        string_array = pa.array(["foo", "bar"], type=pa.string())
        table = pa.Table.from_arrays([int_array, string_array], names=["i", "s"])

        df = create_parquet_table(spark_session, "mytesttable", table)

        with utilizes_valid_plans(df):
            outcome = df.select(named_struct(lit("a"), col("s"), lit("b"), col("i"))).collect()

        assertDataFrameEqual(outcome, expected)

    def test_concat(self, users_dataframe):
        expected = [
            Row(a="user669344115Joshua Browntrue"),
            Row(a="user849118289Brooke Jonesfalse"),
            Row(a="user954079192Collin Frankfalse"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = (
                users_dataframe.select(concat("user_id", "name", "paid_for_service"))
                .limit(3)
                .collect()
            )

        assertDataFrameEqual(outcome, expected)

    def test_equal_null(self, spark_session):
        expected = [
            Row(f1=12.0, f2=-12.0, a=False),
            Row(f1=42.0, f2=42.0, a=True),
            Row(f1=63.0, f2=float("NaN"), a=False),
            Row(f1=None, f2=None, a=True),
        ]

        float1_array = pa.array([63, 42.0, None, 12], type=pa.float64())
        float2_array = pa.array([float("NaN"), 42.0, None, -12], type=pa.float64())
        table = pa.Table.from_arrays([float1_array, float2_array], names=["f1", "f2"])

        df = create_parquet_table(spark_session, "mytesttable", table)

        with utilizes_valid_plans(df):
            outcome = df.select("f1", "f2", equal_null("f1", "f2")).collect()
            assertDataFrameEqual(outcome, expected)

    def test_ifnull(self, spark_session):
        expected = [
            Row(f1=12.0, f2=-12.0, a=12.0),
            Row(f1=None, f2=42.0, a=42.0),
            Row(f1=float("NaN"), f2=63.0, a=float("NaN")),
            Row(f1=None, f2=None, a=None),
        ]

        float1_array = pa.array([float("NaN"), None, None, 12], type=pa.float64())
        float2_array = pa.array([63.0, 42.0, None, -12], type=pa.float64())
        table = pa.Table.from_arrays([float1_array, float2_array], names=["f1", "f2"])

        df = create_parquet_table(spark_session, "mytesttable", table)

        with utilizes_valid_plans(df):
            outcome = df.select("f1", "f2", ifnull("f1", "f2")).collect()
            assertDataFrameEqual(outcome, expected)

    def test_isnotnull(self, spark_session):
        expected = [
            Row(f1=12.0, a=True),
            Row(f1=None, a=False),
            Row(f1=float("NaN"), a=True),
        ]

        float_array = pa.array([float("NaN"), None, 12], type=pa.float64())
        table = pa.Table.from_arrays([float_array], names=["f"])

        df = create_parquet_table(spark_session, "mytesttable", table)

        with utilizes_valid_plans(df):
            outcome = df.select("f", isnotnull("f")).collect()
            assertDataFrameEqual(outcome, expected)

    def test_nullif(self, spark_session):
        expected = [
            Row(f1=12.0, f2=-12.0, a=12.0),
            Row(f1=12.0, f2=12.0, a=None),
            Row(f1=None, f2=42.0, a=None),
            Row(f1=42.0, f2=42.0, a=None),
            Row(f1=float("NaN"), f2=63.0, a=float("NaN")),
            Row(f1=None, f2=None, a=None),
        ]

        float1_array = pa.array([float("NaN"), 42.0, None, None, 12, 12], type=pa.float64())
        float2_array = pa.array([63.0, 42.0, 42.0, None, -12, 12], type=pa.float64())
        table = pa.Table.from_arrays([float1_array, float2_array], names=["f1", "f2"])

        df = create_parquet_table(spark_session, "mytesttable", table)

        with utilizes_valid_plans(df):
            outcome = df.select("f1", "f2", nullif("f1", "f2")).collect()
            assertDataFrameEqual(outcome, expected)

    def test_nvl(self, spark_session):
        expected = [
            Row(f1=float("NaN"), f2=63.0, a=float("NaN")),
            Row(f1=None, f2=None, a=None),
            Row(f1=None, f2=42.0, a=42.0),
            Row(f1=12.0, f2=-12.0, a=12.0),
            Row(f1=12.0, f2=12.0, a=12.0),
        ]

        float1_array = pa.array([float("NaN"), None, None, 12, 12], type=pa.float64())
        float2_array = pa.array([63.0, 42.0, None, -12, 12], type=pa.float64())
        table = pa.Table.from_arrays([float1_array, float2_array], names=["f1", "f2"])

        df = create_parquet_table(spark_session, "mytesttable", table)

        with utilizes_valid_plans(df):
            outcome = df.select("f1", "f2", nvl("f1", "f2")).collect()
            assertDataFrameEqual(outcome, expected)

    def test_nvl2(self, spark_session):
        expected = [
            Row(f1=float("NaN"), f2=63.0, f3=1.0, a=63.0),
            Row(f1=None, f2=42.0, f3=2.0, a=2.0),
            Row(f1=None, f2=None, f3=3.0, a=3.0),
            Row(f1=12.0, f2=-12.0, f3=4.0, a=-12.0),
        ]

        float1_array = pa.array([float("NaN"), None, None, 12], type=pa.float64())
        float2_array = pa.array([63.0, 42.0, None, -12], type=pa.float64())
        float3_array = pa.array([1, 2, 3, 4], type=pa.float64())
        table = pa.Table.from_arrays(
            [float1_array, float2_array, float3_array], names=["f1", "f2", "f3"]
        )

        df = create_parquet_table(spark_session, "mytesttable", table)

        with utilizes_valid_plans(df):
            outcome = df.select("f1", "f2", "f3", nvl2("f1", "f2", "f3")).collect()
            assertDataFrameEqual(outcome, expected)

    def test_bit_length(self, users_dataframe):
        expected = [
            Row(name="Brooke Jones", a=96),
            Row(name="Collin Frank", a=96),
            Row(name="Joshua Brown", a=96),
            Row(name="Mrs. Sheila Jones", a=136),
            Row(name="Rebecca Valentine", a=136),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select("name", bit_length("name")).limit(5).collect()

        assertDataFrameEqual(outcome, expected)

    def test_btrim(self, users_dataframe):
        expected = [
            Row(name="rooke Jone"),
            Row(name="Collin Frank"),
            Row(name="Joshua Brown"),
            Row(name="Mrs. Sheila Jone"),
            Row(name="Rebecca Valentine"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(btrim("name", lit("Bs"))).limit(5).collect()

        assertDataFrameEqual(outcome, expected)

    def test_character_length(self, users_dataframe):
        expected = [
            Row(name="Brooke Jones", a=12),
            Row(name="Collin Frank", a=12),
            Row(name="Joshua Brown", a=12),
            Row(name="Mrs. Sheila Jones", a=17),
            Row(name="Rebecca Valentine", a=17),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select("name", character_length("name")).limit(5).collect()

        assertDataFrameEqual(outcome, expected)

    def test_char_length(self, users_dataframe):
        expected = [
            Row(name="Brooke Jones", a=12),
            Row(name="Collin Frank", a=12),
            Row(name="Joshua Brown", a=12),
            Row(name="Mrs. Sheila Jones", a=17),
            Row(name="Rebecca Valentine", a=17),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select("name", char_length("name")).limit(5).collect()

        assertDataFrameEqual(outcome, expected)

    def test_concat_ws(self, users_dataframe):
        expected = [
            Row(a="user669344115|Joshua Brown|true"),
            Row(a="user849118289|Brooke Jones|false"),
            Row(a="user954079192|Collin Frank|false"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = (
                users_dataframe.select(concat_ws("|", "user_id", "name", "paid_for_service"))
                .limit(3)
                .collect()
            )

        assertDataFrameEqual(outcome, expected)

    def test_contains(self, users_dataframe):
        expected = [
            Row(name="Brooke Jones", a=True),
            Row(name="Collin Frank", a=False),
            Row(name="Joshua Brown", a=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = (
                users_dataframe.select("name", contains("name", lit("rook"))).limit(3).collect()
            )

        assertDataFrameEqual(outcome, expected)

    def test_endswith(self, users_dataframe):
        expected = [
            Row(name="Brooke Jones", a=False),
            Row(name="Collin Frank", a=True),
            Row(name="Joshua Brown", a=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = (
                users_dataframe.select("name", endswith("name", lit("Frank"))).limit(3).collect()
            )

        assertDataFrameEqual(outcome, expected)

    def test_instr(self, users_dataframe):
        expected = [
            Row(name="Brooke Jones", a=2),
            Row(name="Collin Frank", a=0),
            Row(name="Joshua Brown", a=0),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select("name", instr("name", "rook")).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_lcase(self, users_dataframe):
        expected = [
            Row(name="brooke jones"),
            Row(name="collin frank"),
            Row(name="joshua brown"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(lcase("name")).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_length(self, users_dataframe):
        expected = [
            Row(name="Brooke Jones", a=12),
            Row(name="Collin Frank", a=12),
            Row(name="Joshua Brown", a=12),
            Row(name="Mrs. Sheila Jones", a=17),
            Row(name="Rebecca Valentine", a=17),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select("name", length("name")).limit(5).collect()

        assertDataFrameEqual(outcome, expected)

    def test_lower(self, users_dataframe):
        expected = [
            Row(name="brooke jones"),
            Row(name="collin frank"),
            Row(name="joshua brown"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(lower("name")).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_left(self, users_dataframe):
        expected = [
            Row(name="Bro"),
            Row(name="Col"),
            Row(name="Jos"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(left("name", lit(3))).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_locate(self, users_dataframe):
        expected = [
            Row(name="Brooke Jones", a=9),
            Row(name="Collin Frank", a=0),
            Row(name="Joshua Brown", a=10),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select("name", locate("o", "name", 5)).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_lpad(self, users_dataframe):
        expected = [
            Row(a="---Brooke Jones"),
            Row(a="---Collin Frank"),
            Row(a="---Joshua Brown"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(lpad("name", 15, "-")).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_ltrim(self, users_dataframe):
        expected = [
            Row(name="Brooke Jones"),
            Row(name="Collin Frank"),
            Row(name="Joshua Brown"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(ltrim(lpad("name", 15, " "))).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_octet_length(self, users_dataframe):
        expected = [
            Row(name="Brooke Jones", a=12),
            Row(name="Collin Frank", a=12),
            Row(name="Joshua Brown", a=12),
            Row(name="Mrs. Sheila Jones", a=17),
            Row(name="Rebecca Valentine", a=17),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select("name", octet_length("name")).limit(5).collect()

        assertDataFrameEqual(outcome, expected)

    def test_position(self, users_dataframe):
        expected = [
            Row(name="Brooke Jones", a=9),
            Row(name="Collin Frank", a=0),
            Row(name="Joshua Brown", a=10),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = (
                users_dataframe.select("name", position(lit("o"), "name", lit(5)))
                .limit(3)
                .collect()
            )

        assertDataFrameEqual(outcome, expected)

    def test_rlike(self, users_dataframe):
        expected = [
            Row(name="Brooke Jones", a=True),
            Row(name="Collin Frank", a=False),
            Row(name="Joshua Brown", a=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select("name", rlike("name", lit("ro*k"))).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_regexp(self, users_dataframe):
        expected = [
            Row(name="Brooke Jones", a=True),
            Row(name="Collin Frank", a=False),
            Row(name="Joshua Brown", a=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select("name", regexp("name", lit("ro*k"))).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_regexp_like(self, users_dataframe):
        expected = [
            Row(name="Brooke Jones", a=True),
            Row(name="Collin Frank", a=False),
            Row(name="Joshua Brown", a=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = (
                users_dataframe.select("name", regexp_like("name", lit("ro*k"))).limit(3).collect()
            )

        assertDataFrameEqual(outcome, expected)

    def test_replace(self, users_dataframe):
        expected = [
            Row(a="Braake Janes"),
            Row(a="Callin Frank"),
            Row(a="Jashua Brawn"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(replace("name", lit("o"), lit("a"))).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_right(self, users_dataframe):
        expected = [
            Row(name="nes"),
            Row(name="ank"),
            Row(name="own"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(right("name", lit(3))).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_ucase(self, users_dataframe):
        expected = [
            Row(name="BROOKE JONES"),
            Row(name="COLLIN FRANK"),
            Row(name="JOSHUA BROWN"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(ucase("name")).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_rpad(self, users_dataframe):
        expected = [
            Row(a="Brooke Jones---"),
            Row(a="Collin Frank---"),
            Row(a="Joshua Brown---"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(rpad("name", 15, "-")).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_repeat(self, users_dataframe):
        expected = [
            Row(a="Brooke JonesBrooke Jones"),
            Row(a="Collin FrankCollin Frank"),
            Row(a="Joshua BrownJoshua Brown"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(repeat("name", 2)).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_rtrim(self, users_dataframe):
        expected = [
            Row(name="Brooke Jones"),
            Row(name="Collin Frank"),
            Row(name="Joshua Brown"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(rtrim(rpad("name", 15, " "))).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_startswith(self, users_dataframe):
        expected = [
            Row(name="Brooke Jones", a=True),
            Row(name="Collin Frank", a=False),
            Row(name="Joshua Brown", a=False),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = (
                users_dataframe.select("name", startswith("name", lit("Bro"))).limit(3).collect()
            )

        assertDataFrameEqual(outcome, expected)

    def test_substr(self, users_dataframe):
        expected = [
            Row(a="oo"),
            Row(a="ll"),
            Row(a="sh"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(substr("name", lit(3), lit(2))).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_substring(self, users_dataframe):
        expected = [
            Row(a="oo"),
            Row(a="ll"),
            Row(a="sh"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(substring("name", 3, 2)).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_trim(self, users_dataframe):
        expected = [
            Row(name="Brooke Jones"),
            Row(name="Collin Frank"),
            Row(name="Joshua Brown"),
            Row(name="Mrs. Sheila Jones"),
            Row(name="Rebecca Valentine"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = (
                users_dataframe.select(trim(lpad(rpad("name", 18, " "), 36, " ")))
                .limit(5)
                .collect()
            )

        assertDataFrameEqual(outcome, expected)

    def test_upper(self, users_dataframe):
        expected = [
            Row(name="BROOKE JONES"),
            Row(name="COLLIN FRANK"),
            Row(name="JOSHUA BROWN"),
        ]

        with utilizes_valid_plans(users_dataframe):
            outcome = users_dataframe.select(upper("name")).limit(3).collect()

        assertDataFrameEqual(outcome, expected)


@pytest.fixture(scope="class")
def numbers_dataframe(spark_session_for_setup):
    data = [
        [float("NaN"), 3.14 / 2, 0.0, 0.0, -1.0, 1.0],
        [42.0, 0.0, 90.0, 1.57, -0.66, 2.718281828],
        [None, -0.5, 135.0, 2.0, -0.5, 8.0],
        [-1.0, -0.6, 180.0, 3.14159, 0.0, 10.0],
        [-2.0, 4.4, 235.0, 5.0, 0.25, 16.0],
        [-3.0, 4.6, 360.0, 6.28318, 0.5, 100.0],
        [-4.0, 81.0, -60.0, -1.0, 1.0, 100000.0],
    ]

    schema = StructType(
        [
            StructField("f1", DoubleType(), True),
            StructField("f2", DoubleType(), True),
            StructField("angles", DoubleType(), True),
            StructField("radians", DoubleType(), True),
            StructField("near_one", DoubleType(), True),
            StructField("powers", DoubleType(), True),
        ]
    )

    test_df = spark_session_for_setup.createDataFrame(data, schema)

    test_df.createOrReplaceTempView("numbers")
    return spark_session_for_setup.table("numbers")


class TestDataFrameAPIMathFunctions:
    """Tests math functions of the dataframe side of SparkConnect."""

    def test_sqrt(self, numbers_dataframe):
        expected = [
            Row(a=1.0),
            Row(a=1.6487212705609156),
            Row(a=2.8284271247461903),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(sqrt("powers")).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_abs(self, numbers_dataframe):
        expected = [
            Row(a=1.57),
            Row(a=0.5),
            Row(a=0),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.abs("f2")).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_ceil(self, numbers_dataframe):
        expected = [
            Row(a=2),
            Row(a=0),
            Row(a=0),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.ceil("f2")).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_ceiling(self, numbers_dataframe):
        expected = [
            Row(a=2),
            Row(a=0),
            Row(a=0),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = (
                numbers_dataframe.select(pyspark.sql.functions.ceiling("f2")).limit(3).collect()
            )

        assertDataFrameEqual(outcome, expected)

    def test_floor(self, numbers_dataframe):
        expected = [
            Row(a=1),
            Row(a=0),
            Row(a=-1),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.floor("f2")).limit(3).collect()

        assertDataFrameEqual(outcome, expected)

    def test_negate(self, numbers_dataframe):
        expected = [
            Row(a=float("NaN")),
            Row(a=-42.0),
            Row(a=None),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = (
                numbers_dataframe.select(pyspark.sql.functions.negate("f1")).limit(3).collect()
            )

        assertDataFrameEqual(outcome, expected)

    def test_negative(self, numbers_dataframe):
        expected = [
            Row(a=float("NaN")),
            Row(a=-42.0),
            Row(a=None),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = (
                numbers_dataframe.select(pyspark.sql.functions.negative("f1")).limit(3).collect()
            )

        assertDataFrameEqual(outcome, expected)

    def test_positive(self, numbers_dataframe):
        expected = [
            Row(a=float("NaN")),
            Row(a=42.0),
            Row(a=None),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = (
                numbers_dataframe.select(pyspark.sql.functions.positive("f1")).limit(3).collect()
            )

        assertDataFrameEqual(outcome, expected)

    def test_rint(self, numbers_dataframe):
        expected = [
            Row(a=2.0),
            Row(a=-0.0),
            Row(a=0.0),
            Row(a=-1.0),
            Row(a=4.0),
            Row(a=5.0),
            Row(a=81.0),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.rint("f2")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_round(self, numbers_dataframe):
        expected = [
            Row(a=2.0),
            Row(a=-1.0),
            Row(a=0.0),
            Row(a=-1.0),
            Row(a=4.0),
            Row(a=5.0),
            Row(a=81.0),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.round("f2")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_bround(self, numbers_dataframe):
        expected = [
            Row(a=2.0),
            Row(a=0.0),
            Row(a=0.0),
            Row(a=-1.0),
            Row(a=4.0),
            Row(a=5.0),
            Row(a=81.0),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.bround("f2")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_toradians(self, numbers_dataframe):
        expected = [
            Row(a=-1.0471975511965976),
            Row(a=0.0),
            Row(a=1.5707963267948966),
            Row(a=2.356194490192345),
            Row(a=3.141592653589793),
            Row(a=4.101523742186674),
            Row(a=6.283185307179586),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.toRadians("angles")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_radians(self, numbers_dataframe):
        expected = [
            Row(a=-1.0471975511965976),
            Row(a=0.0),
            Row(a=1.5707963267948966),
            Row(a=2.356194490192345),
            Row(a=3.141592653589793),
            Row(a=4.101523742186674),
            Row(a=6.283185307179586),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.radians("angles")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_sign(self, numbers_dataframe):
        expected = [
            Row(a=1.0),
            Row(a=0.0),
            Row(a=-1.0),
            Row(a=-1.0),
            Row(a=1.0),
            Row(a=1.0),
            Row(a=1.0),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.sign("f2")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_signum(self, numbers_dataframe):
        expected = [
            Row(a=1.0),
            Row(a=0.0),
            Row(a=-1.0),
            Row(a=-1.0),
            Row(a=1.0),
            Row(a=1.0),
            Row(a=1.0),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.signum("f2")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_acos(self, numbers_dataframe):
        expected = [
            Row(a=0.0),
            Row(a=1.0471975511965976),
            Row(a=1.318116071652818),
            Row(a=1.5707963267948966),
            Row(a=2.0943951023931957),
            Row(a=2.291615087664986),
            Row(a=3.141592653589793),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.acos("near_one")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_acosh(self, numbers_dataframe):
        expected = [
            Row(a=0.0),
            Row(a=1.0471975511965976),
            Row(a=1.318116071652818),
            Row(a=1.5707963267948966),
            Row(a=2.0943951023931957),
            Row(a=2.291615087664986),
            Row(a=3.141592653589793),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.acosh("near_one")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_asin(self, numbers_dataframe):
        expected = [
            Row(a=-1.5707963267948966),
            Row(a=-0.7208187608700897),
            Row(a=-0.5235987755982989),
            Row(a=0.0),
            Row(a=0.25268025514207865),
            Row(a=0.5235987755982989),
            Row(a=1.5707963267948966),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.asin("near_one")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_asinh(self, numbers_dataframe):
        expected = [
            Row(a=-0.8813735870195428),
            Row(a=-0.6195895837234842),
            Row(a=-0.48121182505960336),
            Row(a=0.0),
            Row(a=0.24746646154726346),
            Row(a=0.48121182505960347),
            Row(a=0.8813735870195429),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.asinh("near_one")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_atan(self, numbers_dataframe):
        expected = [
            Row(a=-0.7853981633974483),
            Row(a=-0.583373006993856),
            Row(a=-0.4636476090008061),
            Row(a=0.0),
            Row(a=0.24497866312686414),
            Row(a=0.4636476090008061),
            Row(a=0.7853981633974483),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.atan("near_one")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_atanh(self, numbers_dataframe):
        expected = [
            Row(a=float("-inf")),
            Row(a=-0.7928136318701909),
            Row(a=-0.5493061443340548),
            Row(a=0.0),
            Row(a=0.25541281188299536),
            Row(a=0.5493061443340548),
            Row(a=float("inf")),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.atanh("near_one")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_atan2(self, numbers_dataframe):
        expected = [
            Row(a=-0.04934263225296999),
            Row(a=-0.4266274931268761),
            Row(a=-0.5779019369622457),
            Row(a=-2.1112158270654806),
            Row(a=1.5707963267948966),
            Row(a=None),
            Row(a=float("NaN")),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.atan2("f1", "f2")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_cos(self, numbers_dataframe):
        expected = [
            Row(a=-0.4161468365471424),
            Row(a=-0.9999999999964793),
            Row(a=0.0007963267107332633),
            Row(a=0.28366218546322625),
            Row(a=0.5403023058681398),
            Row(a=0.9999999999859169),
            Row(a=1.0),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.cos("radians")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_cosh(self, numbers_dataframe):
        expected = [
            Row(a=1.0),
            Row(a=1.543080634815244),
            Row(a=11.591922629945447),
            Row(a=2.5073466880660993),
            Row(a=267.74534051728284),
            Row(a=3.7621956910836314),
            Row(a=74.20994852478785),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.cosh("radians")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_cot(self, numbers_dataframe):
        expected = [
            Row(a=-0.6420926159343306),
            Row(a=-1.2884855944745672),
            Row(a=-1.830487721712452),
            Row(a=0.6420926159343306),
            Row(a=1.830487721712452),
            Row(a=3.91631736464594),
            Row(a=float("inf")),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.cot("near_one")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_sec(self, numbers_dataframe):
        expected = [
            Row(a=-1.0000000000035207),
            Row(a=-2.402997961722381),
            Row(a=1.0),
            Row(a=1.0000000000140832),
            Row(a=1.8508157176809255),
            Row(a=1255.765989664208),
            Row(a=3.5253200858160887),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.sec("radians")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_sin(self, numbers_dataframe):
        expected = [
            Row(a=-0.8414709848078965),
            Row(a=-0.9589242746631385),
            Row(a=-5.307179586686775e-06),
            Row(a=0.0),
            Row(a=0.9092974268256817),
            Row(a=0.9999996829318346),
            Row(a=2.65358979335273e-06),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.sin("radians")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_sinh(self, numbers_dataframe):
        expected = [
            Row(a=-0.5210953054937474),
            Row(a=-0.7089704999551663),
            Row(a=-1.1752011936438014),
            Row(a=0.0),
            Row(a=0.2526123168081683),
            Row(a=0.5210953054937474),
            Row(a=1.1752011936438014),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.sinh("near_one")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_tan(self, numbers_dataframe):
        expected = [
            Row(a=-1.5574077246549023),
            Row(a=-2.185039863261519),
            Row(a=-2.6535897933620727e-06),
            Row(a=-3.380515006246586),
            Row(a=-5.3071795867615165e-06),
            Row(a=0.0),
            Row(a=1255.7655915007897),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.tan("radians")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_tanh(self, numbers_dataframe):
        expected = [
            Row(a=-0.46211715726000974),
            Row(a=-0.5783634130445059),
            Row(a=-0.7615941559557649),
            Row(a=0.0),
            Row(a=0.24491866240370913),
            Row(a=0.46211715726000974),
            Row(a=0.7615941559557649),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.tanh("near_one")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_exp(self, numbers_dataframe):
        expected = [
            Row(a=0.36787944117144233),
            Row(a=0.5168513344916992),
            Row(a=0.6065306597126334),
            Row(a=1.0),
            Row(a=1.2840254166877414),
            Row(a=1.6487212707001282),
            Row(a=2.7182818284590455),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.exp("near_one")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_ln(self, numbers_dataframe):
        expected = [
            Row(a=0.0),
            Row(a=0.9999999998311266),
            Row(a=2.0794415416798357),
            Row(a=2.302585092994046),
            Row(a=2.772588722239781),
            Row(a=4.605170185988092),
            Row(a=11.512925464970229),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.ln("powers")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_log(self, numbers_dataframe):
        expected = [
            Row(a=0.0),
            Row(a=0.9999999998311266),
            Row(a=2.0794415416798357),
            Row(a=2.302585092994046),
            Row(a=2.772588722239781),
            Row(a=4.605170185988092),
            Row(a=11.512925464970229),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.log("powers")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_log10(self, numbers_dataframe):
        expected = [
            Row(a=0.0),
            Row(a=0.43429448182991104),
            Row(a=0.9030899869919435),
            Row(a=1.0),
            Row(a=1.2041199826559248),
            Row(a=2.0),
            Row(a=5.0),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.log10("powers")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_log1p(self, numbers_dataframe):
        expected = [
            Row(a=0.6931471805599453),
            Row(a=1.3132616873947665),
            Row(a=2.1972245773362196),
            Row(a=2.3978952727983707),
            Row(a=2.833213344056216),
            Row(a=4.61512051684126),
            Row(a=11.51293546492023),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.log1p("powers")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_log2(self, numbers_dataframe):
        expected = [
            Row(a=0.0),
            Row(a=1.4426950406453307),
            Row(a=3.0),
            Row(a=3.3219280948873626),
            Row(a=4.0),
            Row(a=6.643856189774725),
            Row(a=16.609640474436812),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(pyspark.sql.functions.log2("powers")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_pow(self, numbers_dataframe):
        expected = [
            Row(a=0.023809523809523808),
            Row(a=0.08485070758702981),
            Row(a=0.1543033499620919),
            Row(a=1.0),
            Row(a=2.5457298950218306),
            Row(a=6.48074069840786),
            Row(a=42.0),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(
                pyspark.sql.functions.pow(lit(42), "near_one")
            ).collect()

        assertDataFrameEqual(outcome, expected)

    def test_power(self, numbers_dataframe):
        expected = [
            Row(a=0.023809523809523808),
            Row(a=0.08485070758702981),
            Row(a=0.1543033499620919),
            Row(a=1.0),
            Row(a=2.5457298950218306),
            Row(a=6.48074069840786),
            Row(a=42.0),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(
                pyspark.sql.functions.power(lit(42), "near_one")
            ).collect()

        assertDataFrameEqual(outcome, expected)

    def test_try_add(self, numbers_dataframe):
        expected = [
            Row(a=-1.0),
            Row(a=-59.0),
            Row(a=134.5),
            Row(a=180.0),
            Row(a=235.25),
            Row(a=360.5),
            Row(a=89.34),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(
                pyspark.sql.functions.try_add("angles", "near_one")
            ).collect()

        assertDataFrameEqual(outcome, expected)

    def test_try_avg(self, numbers_dataframe):
        expected = [
            Row(a=-0.058571428571428594),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            # TODO -- Also test with the incorrect behavior of select instead of agg.
            outcome = numbers_dataframe.agg(pyspark.sql.functions.try_avg("near_one")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_try_divide(self, numbers_dataframe):
        expected = [
            Row(a=-0.0),
            Row(a=-136.36363636363635),
            Row(a=-270.0),
            Row(a=-60.0),
            Row(a=720.0),
            Row(a=940.0),
            Row(a=None),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(
                pyspark.sql.functions.try_divide("angles", "near_one")
            ).collect()

        assertDataFrameEqual(outcome, expected)

    def test_try_multiply(self, numbers_dataframe):
        expected = [
            Row(a=-0.0),
            Row(a=-59.400000000000006),
            Row(a=-60.0),
            Row(a=-67.5),
            Row(a=0.0),
            Row(a=180.0),
            Row(a=58.75),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(
                pyspark.sql.functions.try_multiply("angles", "near_one")
            ).collect()

        assertDataFrameEqual(outcome, expected)

    def test_try_subtract(self, numbers_dataframe):
        expected = [
            Row(a=-61.0),
            Row(a=1.0),
            Row(a=135.5),
            Row(a=180.0),
            Row(a=234.75),
            Row(a=359.5),
            Row(a=90.66),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            outcome = numbers_dataframe.select(
                pyspark.sql.functions.try_subtract("angles", "near_one")
            ).collect()

        assertDataFrameEqual(outcome, expected)

    def test_try_sum(self, numbers_dataframe):
        expected = [
            Row(a=-0.41000000000000014),
        ]

        with utilizes_valid_plans(numbers_dataframe):
            # TODO -- Also test with the incorrect behavior of select instead of agg.
            outcome = numbers_dataframe.agg(pyspark.sql.functions.try_sum("near_one")).collect()

        assertDataFrameEqual(outcome, expected)


class TestDataFrameAggregateBehavior:
    """Tests aggregation behavior using the dataframe side of SparkConnect."""

    def test_literal(self, register_tpch_dataset, spark_session):
        expected = [
            Row(i=1, a=42),
        ]

        with utilizes_valid_plans(spark_session):
            lineitem = spark_session.table("lineitem")

            outcome = lineitem.groupBy(lit(1)).agg(lit(42)).collect()

        assertDataFrameEqual(outcome, expected)

    def test_simple(self, register_tpch_dataset, spark_session):
        expected = [
            Row(i=1, a=Decimal("229577310901.20")),
        ]

        with utilizes_valid_plans(spark_session):
            lineitem = spark_session.table("lineitem")

            outcome = lineitem.groupBy(lit(1)).agg(try_sum("l_extendedprice").alias("a")).collect()

        assertDataFrameEqual(outcome, expected)

    def test_exterior_calculation(self, register_tpch_dataset, spark_session):
        expected = [
            Row(i=1, a=Decimal("459154621802.40")),
        ]

        with utilizes_valid_plans(spark_session):
            lineitem = spark_session.table("lineitem")

            outcome = (
                lineitem.groupBy(lit(1)).agg((2 * try_sum("l_extendedprice")).alias("a")).collect()
            )

        assertDataFrameEqual(outcome, expected)

    def test_exterior_calculation_with_deep_aggregate(self, register_tpch_dataset, spark_session):
        expected = [
            Row(i=1, a=Decimal("1377463865407.20")),
        ]

        with utilizes_valid_plans(spark_session):
            lineitem = spark_session.table("lineitem")

            outcome = (
                lineitem.groupBy(lit(1))
                .agg((3 * (2 * try_sum("l_extendedprice"))).alias("a"))
                .collect()
            )

        assertDataFrameEqual(outcome, expected)

    def test_interior_calculation(self, register_tpch_dataset, spark_session):
        expected = [
            Row(i=1, a=Decimal("229583312116.20")),
        ]

        with utilizes_valid_plans(spark_session):
            lineitem = spark_session.table("lineitem")

            outcome = (
                lineitem.groupBy(lit(1))
                .agg(try_sum(col("l_extendedprice") + 1).alias("a"))
                .collect()
            )

        assertDataFrameEqual(outcome, expected)

    def test_computation_with_two_aggregations(self, register_tpch_dataset, spark_session):
        expected = [
            Row(l_suppkey=1, a=Decimal("390311321186.4300")),
            Row(l_suppkey=2, a=Decimal("288371456352.7200")),
            Row(l_suppkey=3, a=Decimal("306540435762.9600")),
        ]

        with utilizes_valid_plans(spark_session):
            lineitem = spark_session.table("lineitem")

            outcome = (
                lineitem.groupBy("l_suppkey")
                .agg((try_sum(col("l_extendedprice")) * try_sum(col("l_quantity"))).alias("a"))
                .orderBy("l_suppkey")
                .limit(3)
                .collect()
            )

        assertDataFrameEqual(outcome, expected)

    def test_computation_with_two_aggregations_and_internal_calculation(
        self, register_tpch_dataset, spark_session
    ):
        expected = [
            Row(l_suppkey=1, a=Decimal("3903113211864.3000")),
            Row(l_suppkey=2, a=Decimal("2883714563527.2000")),
            Row(l_suppkey=3, a=Decimal("3065404357629.6000")),
        ]

        with utilizes_valid_plans(spark_session):
            lineitem = spark_session.table("lineitem")

            outcome = (
                lineitem.groupBy("l_suppkey")
                .agg((try_sum(col("l_extendedprice") * 10) * try_sum(col("l_quantity"))).alias("a"))
                .orderBy("l_suppkey")
                .limit(3)
                .collect()
            )

        assertDataFrameEqual(outcome, expected)

    def test_multiple_measures(self, register_tpch_dataset, spark_session):
        expected = [
            Row(
                i=1,
                a=Decimal("229583312116.20"),
                b=Decimal("229589313331.20"),
                c=Decimal("229595314546.20"),
            ),
        ]

        with utilizes_valid_plans(spark_session):
            lineitem = spark_session.table("lineitem")

            outcome = (
                lineitem.groupBy(lit(1))
                .agg(
                    try_sum(col("l_extendedprice") + 1).alias("a"),
                    try_sum(col("l_extendedprice") + 2).alias("b"),
                    try_sum(col("l_extendedprice") + 3).alias("c"),
                )
                .collect()
            )

        assertDataFrameEqual(outcome, expected)

    def test_multiple_measures_and_calculations(self, register_tpch_dataset, spark_session):
        expected = [
            Row(
                l_suppkey=1,
                a=Decimal("48255093.18"),
                b=Decimal("72383889.77"),
                c=Decimal("24129421.59"),
            ),
            Row(
                l_suppkey=2,
                a=Decimal("40764978.28"),
                b=Decimal("61148581.42"),
                c=Decimal("20384160.14"),
            ),
            Row(
                l_suppkey=3,
                a=Decimal("42380815.12"),
                b=Decimal("63572396.68"),
                c=Decimal("21192168.56"),
            ),
        ]

        with utilizes_valid_plans(spark_session):
            lineitem = spark_session.table("lineitem")

            outcome = (
                lineitem.groupBy("l_suppkey")
                .agg(
                    try_sum(2 * col("l_extendedprice")).alias("a"),
                    try_sum(3 * col("l_extendedprice") + 2).alias("b"),
                    try_sum(col("l_extendedprice") + 3).alias("c"),
                )
                .orderBy("l_suppkey")
                .limit(3)
                .collect()
            )

        assertDataFrameEqual(outcome, expected)


class TestDataFrameWindowFunctions:
    """Tests window functions of the dataframe side of SparkConnect."""

    def test_row_number(self, users_dataframe):
        expected = [
            Row(user_id="user705452451", name="Adrian Reyes", paid_for_service=False, row_number=1),
            Row(user_id="user406489700", name="Alan Aguirre DVM", paid_for_service=False,
                row_number=2),
            Row(user_id="user965620978", name="Alan Whitaker", paid_for_service=False,
                row_number=3),
        ]

        with utilizes_valid_plans(users_dataframe):
            window_spec = Window.partitionBy().orderBy("name")
            outcome = users_dataframe.withColumn("row_number",
                                                 row_number().over(window_spec)).orderBy(
                "row_number").limit(3)

        assertDataFrameEqual(outcome, expected)
