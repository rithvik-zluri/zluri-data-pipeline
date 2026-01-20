import pytest
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, LongType, TimestampType
)

from src.pipelines.agents.agents_transform import transform_agents


# ------------------------------------------------------
# Spark fixture
# ------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("agents-transform-tests")
        .getOrCreate()
    )


# ------------------------------------------------------
# FULL SCHEMAS (MATCH TRANSFORM)
# ------------------------------------------------------
contact_schema = StructType([
    StructField("email", StringType(), True),
    StructField("name", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("language", StringType(), True),
    StructField("mobile", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("time_zone", StringType(), True),
    StructField("last_login_at", TimestampType(), True),
])

agent_schema = StructType([
    StructField("id", LongType(), True),
    StructField("contact", contact_schema, True),
    StructField("available", BooleanType(), True),
])


agents_schema = StructType([
    StructField("id", LongType(), True),
])


# ------------------------------------------------------
# Helpers
# ------------------------------------------------------
def make_agent(
    agent_id=1,
    email="a@b.com",
    name="Agent A",
    available=True,
):
    return Row(
        id=agent_id,
        contact=Row(
            email=email,
            name=name,
            job_title="Support",
            language="en",
            mobile="123",
            phone="456",
            time_zone="UTC",
            last_login_at=None,
        ),
        available=available,
    )


def agent_details_df(spark, rows):
    return spark.createDataFrame(rows, schema=agent_schema)


def agents_df(spark, ids):
    return spark.createDataFrame(
        [Row(id=i) for i in ids],
        schema=agents_schema
    )


# ======================================================
# ACTIVE AGENT
# ======================================================
def test_active_agent_status(spark):
    df = agent_details_df(spark, [make_agent(1)])
    active_df = agents_df(spark, [1])

    final_df, error_df = transform_agents(df, active_df)

    assert final_df.count() == 1
    assert final_df.collect()[0]["status"] == "active"
    assert error_df.count() == 0


# ======================================================
# INACTIVE AGENT
# ======================================================
def test_inactive_agent_status(spark):
    df = agent_details_df(spark, [make_agent(2)])
    active_df = agents_df(spark, [])

    final_df, _ = transform_agents(df, active_df)

    assert final_df.collect()[0]["status"] == "inactive"


# ======================================================
# ensure_column → missing optional columns
# ======================================================
def test_missing_optional_columns_are_added(spark):
    df = spark.createDataFrame(
        [
            Row(
                id=3,
                contact=Row(
                    email="x@y.com",
                    name="X",
                    job_title=None,
                    language=None,
                    mobile=None,
                    phone=None,
                    time_zone=None,
                    last_login_at=None,
                )
            )
        ],
        schema=StructType([
            StructField("id", LongType(), True),
            StructField("contact", contact_schema, True),
        ])
    )

    final_df, error_df = transform_agents(df, agents_df(spark, [3]))

    for col in [
        "ticket_scope",
        "org_agent_id",
        "signature",
        "freshchat_agent",
    ]:
        assert col in final_df.columns

    assert error_df.count() == 0


# ======================================================
# ERROR: ID IS NULL
# ======================================================
def test_error_when_id_is_null(spark):
    df = agent_details_df(
        spark,
        [make_agent(agent_id=None)]
    )

    final_df, error_df = transform_agents(df, agents_df(spark, []))

    assert final_df.count() == 0
    assert error_df.count() == 1
    assert error_df.collect()[0]["error_type"] == "MISSING_REQUIRED_FIELD"


# ======================================================
# ERROR: EMAIL IS NULL
# ======================================================
def test_error_when_email_is_null(spark):
    df = agent_details_df(
        spark,
        [make_agent(agent_id=4, email=None)]
    )

    _, error_df = transform_agents(df, agents_df(spark, []))
    assert error_df.count() == 1


# ======================================================
# ERROR: NAME IS NULL
# ======================================================
def test_error_when_name_is_null(spark):
    df = agent_details_df(
        spark,
        [make_agent(agent_id=5, name=None)]
    )

    _, error_df = transform_agents(df, agents_df(spark, []))
    assert error_df.count() == 1


# ======================================================
# EMPTY INPUT
# ======================================================
def test_empty_input(spark):
    empty_df = spark.createDataFrame([], schema=agent_schema)
    final_df, error_df = transform_agents(empty_df, agents_df(spark, []))

    assert final_df.count() == 0
    assert error_df.count() == 0


# ======================================================
# WRONG TYPE CASTING
# ======================================================

def test_error_when_id_is_non_numeric(spark):
    df = spark.createDataFrame(
        [Row(id="abc", contact=make_agent().contact)],
        schema=StructType([
            StructField("id", StringType(), True),
            StructField("contact", contact_schema, True),
        ])
    )

    final_df, error_df = transform_agents(df, agents_df(spark, []))

    assert final_df.count() == 0
    assert error_df.count() == 1


# ======================================================
# WHEN OPTIONAL FIELDS ARE IGNORED
# ======================================================

def test_extra_fields_are_ignored(spark):
    df = spark.createDataFrame(
        [
            Row(
                id=20,
                contact=make_agent().contact,
                random_field="junk"
            )
        ],
        schema=StructType([
            StructField("id", LongType(), True),
            StructField("contact", contact_schema, True),
            StructField("random_field", StringType(), True),
        ])
    )

    final_df, error_df = transform_agents(df, agents_df(spark, [20]))

    assert final_df.count() == 1
    assert error_df.count() == 0

# ======================================================
# PARTIAL OPTIONAL FIELD TYPE MISMATCH
# ======================================================
def test_optional_field_type_mismatch_does_not_fail(spark):
    df = spark.createDataFrame(
        [
            Row(
                id=30,
                contact=make_agent().contact,
                available="yes"
            )
        ],
        schema=StructType([
            StructField("id", LongType(), True),
            StructField("contact", contact_schema, True),
            StructField("available", StringType(), True),
        ])
    )

    final_df, error_df = transform_agents(df, agents_df(spark, [30]))

    assert final_df.count() == 1
    assert error_df.count() == 0
