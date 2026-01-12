import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, BooleanType
from src.pipelines.agents.agents_pipeline import transform_agents

# -----------------------------
# Spark Fixture
# -----------------------------
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .master("local[1]") \
        .appName("agents-pipeline-test") \
        .getOrCreate()

# -----------------------------
# Schemas
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
# 1. Valid Active Agent (Day2 contains agent)
# -----------------------------
def test_valid_active_agent(spark):
    agent_details_data = [{
        "id": 1,
        "contact": {"email": "a@test.com", "name": "Agent A", "job_title": None, "language": "en", "mobile": None, "phone": None, "time_zone": "UTC"},
        "available": True, "deactivated": False, "focus_mode": False,
        "agent_operational_status": "available", "last_active_at": "2024-01-01T10:00:00Z",
        "created_at": "2023-01-01T10:00:00Z", "updated_at": "2024-01-01T10:00:00Z"
    }]

    agents_data = [{"id": 1}]

    final_df, error_df = transform_agents(
        spark.createDataFrame(agent_details_data, agent_details_schema),
        spark.createDataFrame(agents_data, agents_schema)
    )

    assert final_df.count() == 1
    assert final_df.collect()[0]["status"] == "active"
    assert error_df.count() == 0


# -----------------------------
# 2. Inactive Agent (Missing in Day2)
# -----------------------------
def test_inactive_agent_missing_in_day2(spark):
    agent_details_data = [{
        "id": 2,
        "contact": {"email": "b@test.com", "name": "Agent B", "job_title": None, "language": None, "mobile": None, "phone": None, "time_zone": None},
        "available": True, "deactivated": False, "focus_mode": False,
        "agent_operational_status": None, "last_active_at": None,
        "created_at": None, "updated_at": None
    }]

    # Day2 agents list empty → agent missing
    final_df, _ = transform_agents(
        spark.createDataFrame(agent_details_data, agent_details_schema),
        spark.createDataFrame([], agents_schema)
    )

    assert final_df.count() == 1
    assert final_df.collect()[0]["status"] == "inactive"


# -----------------------------
# 3. Missing ID → Error
# -----------------------------
def test_missing_id_goes_to_error(spark):
    agent_details_data = [{
        "id": None,
        "contact": {"email": "c@test.com", "name": "Agent C", "job_title": None, "language": None, "mobile": None, "phone": None, "time_zone": None},
        "available": None, "deactivated": None, "focus_mode": None,
        "agent_operational_status": None, "last_active_at": None,
        "created_at": None, "updated_at": None
    }]

    final_df, error_df = transform_agents(
        spark.createDataFrame(agent_details_data, agent_details_schema),
        spark.createDataFrame([], agents_schema)
    )

    assert final_df.count() == 0
    assert error_df.count() == 1
    assert error_df.collect()[0]["error_type"] == "MISSING_REQUIRED_FIELD"


# -----------------------------
# 4. Missing Email → Error
# -----------------------------
def test_missing_email_goes_to_error(spark):
    agent_details_data = [{
        "id": 3,
        "contact": {"email": None, "name": "Agent D", "job_title": None, "language": None, "mobile": None, "phone": None, "time_zone": None},
        "available": None, "deactivated": None, "focus_mode": None,
        "agent_operational_status": None, "last_active_at": None,
        "created_at": None, "updated_at": None
    }]

    final_df, error_df = transform_agents(
        spark.createDataFrame(agent_details_data, agent_details_schema),
        spark.createDataFrame([], agents_schema)
    )

    assert final_df.count() == 0
    assert error_df.count() == 1


# -----------------------------
# 5. Missing Name → Error
# -----------------------------
def test_missing_name_goes_to_error(spark):
    agent_details_data = [{
        "id": 4,
        "contact": {"email": "d@test.com", "name": None, "job_title": None, "language": None, "mobile": None, "phone": None, "time_zone": None},
        "available": None, "deactivated": None, "focus_mode": None,
        "agent_operational_status": None, "last_active_at": None,
        "created_at": None, "updated_at": None
    }]

    final_df, error_df = transform_agents(
        spark.createDataFrame(agent_details_data, agent_details_schema),
        spark.createDataFrame([], agents_schema)
    )

    assert final_df.count() == 0
    assert error_df.count() == 1


# -----------------------------
# 6. Mixed Valid + Invalid
# -----------------------------
def test_mixed_valid_and_invalid_agents(spark):
    agent_details_data = [
        {
            "id": 5,
            "contact": {"email": "e@test.com", "name": "Agent E", "job_title": None, "language": None, "mobile": None, "phone": None, "time_zone": None},
            "available": True, "deactivated": False, "focus_mode": None,
            "agent_operational_status": None, "last_active_at": None,
            "created_at": None, "updated_at": None
        },
        {
            "id": None,
            "contact": {"email": None, "name": None, "job_title": None, "language": None, "mobile": None, "phone": None, "time_zone": None},
            "available": None, "deactivated": None, "focus_mode": None,
            "agent_operational_status": None, "last_active_at": None,
            "created_at": None, "updated_at": None
        }
    ]

    agents_data = [{"id": 5}]

    final_df, error_df = transform_agents(
        spark.createDataFrame(agent_details_data, agent_details_schema),
        spark.createDataFrame(agents_data, agents_schema)
    )

    assert final_df.count() == 1
    assert error_df.count() == 1
    assert final_df.collect()[0]["status"] == "active"


# -----------------------------
# 7. Duplicate Agent IDs
# -----------------------------
def test_duplicate_agent_ids(spark):
    agent_details_data = [
        {
            "id": 6,
            "contact": {"email": "f1@test.com", "name": "Agent F1", "job_title": None, "language": None, "mobile": None, "phone": None, "time_zone": None},
            "available": True, "deactivated": False, "focus_mode": None,
            "agent_operational_status": None, "last_active_at": None,
            "created_at": None, "updated_at": None
        },
        {
            "id": 6,
            "contact": {"email": "f2@test.com", "name": "Agent F2", "job_title": None, "language": None, "mobile": None, "phone": None, "time_zone": None},
            "available": True, "deactivated": False, "focus_mode": None,
            "agent_operational_status": None, "last_active_at": None,
            "created_at": None, "updated_at": None
        }
    ]

    agents_data = [{"id": 6}]

    final_df, error_df = transform_agents(
        spark.createDataFrame(agent_details_data, agent_details_schema),
        spark.createDataFrame(agents_data, agents_schema)
    )

    assert final_df.count() == 2
    assert error_df.count() == 0


# -----------------------------
# 8. Extra Agent in Day2 Ignored
# -----------------------------
def test_extra_agent_in_day2_is_ignored(spark):
    agent_details_data = [{
        "id": 7,
        "contact": {"email": "g@test.com", "name": "Agent G", "job_title": None, "language": None, "mobile": None, "phone": None, "time_zone": None},
        "available": True, "deactivated": False, "focus_mode": None,
        "agent_operational_status": None, "last_active_at": None,
        "created_at": None, "updated_at": None
    }]

    agents_data = [{"id": 7}, {"id": 999}]

    final_df, error_df = transform_agents(
        spark.createDataFrame(agent_details_data, agent_details_schema),
        spark.createDataFrame(agents_data, agents_schema)
    )

    assert final_df.count() == 1
    assert final_df.collect()[0]["agent_id"] == 7


# -----------------------------
# 9. Null Contact Struct → Error
# -----------------------------
def test_null_contact_struct(spark):
    agent_details_data = [{
        "id": 13,
        "contact": None,
        "available": True, "deactivated": False, "focus_mode": False,
        "agent_operational_status": "available", "last_active_at": None,
        "created_at": None, "updated_at": None
    }]

    final_df, error_df = transform_agents(
        spark.createDataFrame(agent_details_data, agent_details_schema),
        spark.createDataFrame([], agents_schema)
    )

    assert final_df.count() == 0
    assert error_df.count() == 1


# -----------------------------
# 10. Empty Inputs
# -----------------------------
def test_empty_inputs(spark):
    final_df, error_df = transform_agents(
        spark.createDataFrame([], agent_details_schema),
        spark.createDataFrame([], agents_schema)
    )

    assert final_df.count() == 0
    assert error_df.count() == 0
