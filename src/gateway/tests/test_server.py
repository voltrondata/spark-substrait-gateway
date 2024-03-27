# SPDX-License-Identifier: Apache-2.0
"""Tests for the Spark to Substrait Gateway server."""
from hamcrest import assert_that, equal_to
from pyspark import Row
from pyspark.sql.functions import col, substring
from pyspark.testing import assertDataFrameEqual


# pylint: disable=missing-function-docstring
class TestDataFrameAPI:
    """Tests of the dataframe side of SparkConnect."""

    # pylint: disable=singleton-comparison
    def test_filter(self, users_dataframe):
        outcome = users_dataframe.filter(col('paid_for_service') == True).collect()
        assert len(outcome) == 29

    # pylint: disable=singleton-comparison
    def test_filter_with_show(self, users_dataframe, capsys):
        expected = '''+-------------+---------------+----------------+
|      user_id|           name|paid_for_service|
+-------------+---------------+----------------+
|user669344115|   Joshua Brown|            true|
|user282427709|Michele Carroll|            true|
+-------------+---------------+----------------+

'''
        users_dataframe.filter(col('paid_for_service') == True).limit(2).show()
        outcome = capsys.readouterr().out
        assert_that(outcome, equal_to(expected))

    # pylint: disable=singleton-comparison
    def test_filter_with_show_with_limit(self, users_dataframe, capsys):
        expected = '''+-------------+------------+----------------+
|      user_id|        name|paid_for_service|
+-------------+------------+----------------+
|user669344115|Joshua Brown|            true|
+-------------+------------+----------------+
only showing top 1 row

'''
        users_dataframe.filter(col('paid_for_service') == True).show(1)
        outcome = capsys.readouterr().out
        assert_that(outcome, equal_to(expected))

    # pylint: disable=singleton-comparison
    def test_filter_with_show_and_truncate(self, users_dataframe, capsys):
        expected = '''+----------+----------+----------------+
|   user_id|      name|paid_for_service|
+----------+----------+----------------+
|user669...|Joshua ...|            true|
+----------+----------+----------------+

'''
        users_dataframe.filter(col('paid_for_service') == True).limit(1).show(truncate=10)
        outcome = capsys.readouterr().out
        assert_that(outcome, equal_to(expected))

    def test_count(self, users_dataframe):
        outcome = users_dataframe.count()
        assert outcome == 100

    def test_limit(self, users_dataframe, spark_session):
        expected = [
            Row(user_id='user849118289', name='Brooke Jones', paid_for_service=False),
            Row(user_id='user954079192', name='Collin Frank', paid_for_service=False),
        ]
        outcome = users_dataframe.limit(2).collect()
        assertDataFrameEqual(outcome, expected)

    def test_with_column(self, users_dataframe, spark_session):
        expected = [
            Row(user_id='user849118289', name='Brooke Jones', paid_for_service=False),
        ]
        outcome = users_dataframe.withColumn(
            'user_id', col('user_id')).limit(1).collect()
        assertDataFrameEqual(outcome, expected)

    def test_cast(self, users_dataframe, spark_session):
        expected = [
            Row(user_id=849, name='Brooke Jones', paid_for_service=False),
        ]
        outcome = users_dataframe.withColumn(
            'user_id',
            substring(col('user_id'), 5, 3).cast('integer')).limit(1).collect()
        assertDataFrameEqual(outcome, expected)
