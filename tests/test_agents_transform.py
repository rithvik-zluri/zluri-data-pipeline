import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, BooleanType, StructType
from src.pipelines.agents_pipeline import transform_agents

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("agents-pipeline-test") \
        .getOrCreate()

# -----------------------------
# Common Schemas
# -----------------------------

agent_details_schema = StructType([
    StructField("id", LongType(), True),
    StructField("contact", StructType([
        StructField("email", StringType(), True),
        StructField("name", StringType(), True),
        StructField("job_title", StringType(), True),
        StructField("language", StringType(), True),
        StructField("mobile", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("time_zone", StringType(), True),
    ]), True),
    StructField("available", BooleanType(), True),
    StructField("deactivated", BooleanType(), True),
    StructField("focus_mode", BooleanType(), True),
    StructField("agent_operational_status", StringType(), True),
    StructField("last_active_at", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
])

agents_schema = StructType([
    StructField("id", LongType(), True)
])

# -----------------------------
# Test 1: Valid Active Agent
# -----------------------------
def test_valid_active_agent(spark):
    agent_details_data = [
        {
            "id": 1,
            "contact": {
                "email": "a@test.com",
                "name": "Agent A",
                "job_title": None,
                "language": "en",
                "mobile": None,
                "phone": None,
                "time_zone": "UTC"
            },
            "available": True,
            "deactivated": False,
            "focus_mode": False,
            "agent_operational_status": "available",
            "last_active_at": "2024-01-01T10:00:00Z",
            "created_at": "2023-01-01T10:00:00Z",
            "updated_at": "2024-01-01T10:00:00Z"
        }
    ]

    agents_data = [{"id": 1}]

    agent_details_df = spark.createDataFrame(agent_details_data, schema=agent_details_schema)
    agents_df = spark.createDataFrame(agents_data, schema=agents_schema)

    final_df, error_df = transform_agents(agent_details_df, agents_df)

    assert final_df.count() == 1
    assert final_df.collect()[0]["status"] == "active"
    assert error_df.count() == 0

# -----------------------------
# Test 2: Inactive Agent
# -----------------------------
def test_inactive_agent(spark):
    agent_details_data = [
        {
            "id": 2,
            "contact": {
                "email": "b@test.com",
                "name": "Agent B",
                "job_title": None,
                "language": None,
                "mobile": None,
                "phone": None,
                "time_zone": None
            },
            "available": True,
            "deactivated": False,
            "focus_mode": False,
            "agent_operational_status": None,
            "last_active_at": None,
            "created_at": None,
            "updated_at": None
        }
    ]

    agent_details_df = spark.createDataFrame(agent_details_data, schema=agent_details_schema)
    agents_df = spark.createDataFrame([], schema=agents_schema)

    final_df, error_df = transform_agents(agent_details_df, agents_df)

    assert final_df.count() == 1
    assert final_df.collect()[0]["status"] == "inactive"

# -----------------------------
# Test 3: Missing ID → Error
# -----------------------------
def test_missing_id_goes_to_error(spark):
    agent_details_data = [
        {
            "id": None,
            "contact": {
                "email": "c@test.com",
                "name": "Agent C",
                "job_title": None,
                "language": None,
                "mobile": None,
                "phone": None,
                "time_zone": None
            },
            "available": None,
            "deactivated": None,
            "focus_mode": None,
            "agent_operational_status": None,
            "last_active_at": None,
            "created_at": None,
            "updated_at": None
        }
    ]

    agent_details_df = spark.createDataFrame(agent_details_data, schema=agent_details_schema)
    agents_df = spark.createDataFrame([], schema=agents_schema)

    final_df, error_df = transform_agents(agent_details_df, agents_df)

    assert final_df.count() == 0
    assert error_df.count() == 1
    assert error_df.collect()[0]["error_type"] == "MISSING_REQUIRED_FIELD"

# -----------------------------
# Test 4: Missing Email → Error
# -----------------------------
def test_missing_email_goes_to_error(spark):
    agent_details_data = [
        {
            "id": 3,
            "contact": {
                "email": None,
                "name": "Agent D",
                "job_title": None,
                "language": None,
                "mobile": None,
                "phone": None,
                "time_zone": None
            },
            "available": None,
            "deactivated": None,
            "focus_mode": None,
            "agent_operational_status": None,
            "last_active_at": None,
            "created_at": None,
            "updated_at": None
        }
    ]

    agent_details_df = spark.createDataFrame(agent_details_data, schema=agent_details_schema)
    agents_df = spark.createDataFrame([], schema=agents_schema)

    final_df, error_df = transform_agents(agent_details_df, agents_df)

    assert final_df.count() == 0
    assert error_df.count() == 1

# -----------------------------
# Test 5: Missing Name → Error
# -----------------------------
def test_missing_name_goes_to_error(spark):
    agent_details_data = [
        {
            "id": 4,
            "contact": {
                "email": "d@test.com",
                "name": None,
                "job_title": None,
                "language": None,
                "mobile": None,
                "phone": None,
                "time_zone": None
            },
            "available": None,
            "deactivated": None,
            "focus_mode": None,
            "agent_operational_status": None,
            "last_active_at": None,
            "created_at": None,
            "updated_at": None
        }
    ]

    agent_details_df = spark.createDataFrame(agent_details_data, schema=agent_details_schema)
    agents_df = spark.createDataFrame([], schema=agents_schema)

    final_df, error_df = transform_agents(agent_details_df, agents_df)

    assert final_df.count() == 0
    assert error_df.count() == 1

# -----------------------------
# Test 6: Mixed Valid + Invalid
# -----------------------------
def test_mixed_valid_and_invalid_agents(spark):
    agent_details_data = [
        {
            "id": 5,
            "contact": {
                "email": "e@test.com",
                "name": "Agent E",
                "job_title": None,
                "language": None,
                "mobile": None,
                "phone": None,
                "time_zone": None
            },
            "available": None,
            "deactivated": False,
            "focus_mode": None,
            "agent_operational_status": None,
            "last_active_at": None,
            "created_at": None,
            "updated_at": None
        },
        {
            "id": None,
            "contact": {
                "email": None,
                "name": None,
                "job_title": None,
                "language": None,
                "mobile": None,
                "phone": None,
                "time_zone": None
            },
            "available": None,
            "deactivated": None,
            "focus_mode": None,
            "agent_operational_status": None,
            "last_active_at": None,
            "created_at": None,
            "updated_at": None
        }
    ]

    agents_data = [{"id": 5}]

    agent_details_df = spark.createDataFrame(agent_details_data, schema=agent_details_schema)
    agents_df = spark.createDataFrame(agents_data, schema=agents_schema)

    final_df, error_df = transform_agents(agent_details_df, agents_df)

    assert final_df.count() == 1
    assert error_df.count() == 1
    assert final_df.collect()[0]["status"] == "active"

# -----------------------------
# Test 7: Duplicate Agent IDs
# -----------------------------
def test_duplicate_agent_ids(spark):
    agent_details_data = [
        {
            "id": 6,
            "contact": {"email": "f1@test.com", "name": "Agent F1", "job_title": None, "language": None, "mobile": None, "phone": None, "time_zone": None},
            "available": True, "deactivated": False, "focus_mode": None, "agent_operational_status": None,
            "last_active_at": None, "created_at": None, "updated_at": None
        },
        {
            "id": 6,
            "contact": {"email": "f2@test.com", "name": "Agent F2", "job_title": None, "language": None, "mobile": None, "phone": None, "time_zone": None},
            "available": True, "deactivated": False, "focus_mode": None, "agent_operational_status": None,
            "last_active_at": None, "created_at": None, "updated_at": None
        }
    ]

    agents_data = [{"id": 6}]

    agent_details_df = spark.createDataFrame(agent_details_data, schema=agent_details_schema)
    agents_df = spark.createDataFrame(agents_data, schema=agents_schema)

    final_df, error_df = transform_agents(agent_details_df, agents_df)

    assert final_df.count() == 2
    assert all(row["status"] == "active" for row in final_df.collect())

# -----------------------------
# Test 8: Active List Has Extra Agent Not In Details
# -----------------------------
def test_active_agent_not_in_details_is_ignored(spark):
    agent_details_data = [
        {
            "id": 7,
            "contact": {"email": "g@test.com", "name": "Agent G", "job_title": None, "language": None, "mobile": None, "phone": None, "time_zone": None},
            "available": True, "deactivated": False, "focus_mode": None, "agent_operational_status": None,
            "last_active_at": None, "created_at": None, "updated_at": None
        }
    ]

    agents_data = [{"id": 7}, {"id": 999}]  # 999 does not exist in details

    agent_details_df = spark.createDataFrame(agent_details_data, schema=agent_details_schema)
    agents_df = spark.createDataFrame(agents_data, schema=agents_schema)

    final_df, error_df = transform_agents(agent_details_df, agents_df)

    assert final_df.count() == 1
    assert final_df.collect()[0]["agent_id"] == 7
