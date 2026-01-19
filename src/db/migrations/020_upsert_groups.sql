-- ============================================================
-- 013_upsert_groups.sql
-- ============================================================

BEGIN;

WITH RECURSIVE

-- ----------------------------
-- Capture execution context
-- ----------------------------
context AS (
    SELECT
        current_setting('app.flow_run_id', true) AS flow_run_id,
        current_setting('app.day', true)         AS day,
        NOW()                                    AS started_at
),

-- ----------------------------
-- UPSERT GROUPS
-- ----------------------------
upserted_groups AS (
    INSERT INTO groups (
        group_id,
        name,
        parent_group_id,
        created_at,
        updated_at
    )
    SELECT
        group_id,
        name,
        parent_group_id,
        created_at,
        updated_at
    FROM stg_groups
    ON CONFLICT (group_id)
    DO UPDATE SET
        name            = EXCLUDED.name,
        parent_group_id = EXCLUDED.parent_group_id,
        updated_at      = EXCLUDED.updated_at
    RETURNING (xmax = 0) AS inserted
),

-- ----------------------------
-- DELETE EXISTING MEMBERSHIP
-- ----------------------------
deleted_membership AS (
    DELETE FROM group_membership
    WHERE group_id IN (
        SELECT DISTINCT group_id FROM stg_group_membership
    )
    RETURNING 1
),

-- ----------------------------
-- INSERT CURRENT MEMBERSHIP
-- ----------------------------
inserted_membership AS (
    INSERT INTO group_membership (
        group_id,
        agent_id
    )
    SELECT
        group_id,
        agent_id
    FROM stg_group_membership
    RETURNING 1
),

-- ----------------------------
-- RECURSIVE GROUP TREE
-- ----------------------------
group_tree AS (
    -- base case
    SELECT
        g.group_id,
        g.parent_group_id,
        g.group_id AS root_group_id
    FROM groups g

    UNION ALL

    -- recursive step
    SELECT
        parent.group_id,
        parent.parent_group_id,
        child.root_group_id
    FROM groups parent
    JOIN group_tree child
      ON parent.group_id = child.parent_group_id
),

-- ----------------------------
-- ACTIVE GROUPS
-- ----------------------------
active_groups AS (
    SELECT DISTINCT gm.group_id
    FROM group_membership gm
    JOIN agents a
      ON gm.agent_id = a.agent_id
    WHERE a.status = 'active'
),

groups_with_active_descendants AS (
    SELECT DISTINCT gt.root_group_id AS group_id
    FROM group_tree gt
    JOIN active_groups ag
      ON gt.group_id = ag.group_id
),

-- ----------------------------
-- UPDATE INACTIVE FLAG
-- ----------------------------
updated_groups AS (
    UPDATE groups
    SET inactive = CASE
        WHEN group_id IN (
            SELECT group_id FROM groups_with_active_descendants
        )
        THEN false
        ELSE true
    END
    RETURNING 1
),

-- ----------------------------
-- METRICS
-- ----------------------------
metrics AS (
    SELECT
        (SELECT COUNT(*) FROM upserted_groups WHERE inserted)     AS rows_inserted,
        (SELECT COUNT(*) FROM upserted_groups WHERE NOT inserted) AS rows_updated,
        (SELECT COUNT(*) FROM deleted_membership)                 AS rows_deleted,
        (SELECT COUNT(*) FROM inserted_membership)                AS membership_inserted,
        (SELECT COUNT(*) FROM updated_groups)                     AS groups_recomputed
),

-- ----------------------------
-- FINISH TIMING
-- ----------------------------
finished AS (
    SELECT NOW() AS finished_at
)

-- ----------------------------
-- PIPELINE METRICS
-- ----------------------------
INSERT INTO pipeline_metrics (
    flow_run_id,
    task_name,
    pipeline_name,
    table_name,
    day,
    rows_inserted,
    rows_updated,
    rows_deleted,
    rows_affected,
    status,
    error_message,
    started_at,
    finished_at,
    duration_seconds
)
SELECT
    c.flow_run_id,
    'groups_task',
    'groups',
    'groups',
    c.day,

    m.rows_inserted,
    m.rows_updated,
    m.rows_deleted,
    m.rows_inserted + m.rows_updated + m.rows_deleted,

    'success',
    NULL,
    c.started_at,
    f.finished_at,
    EXTRACT(EPOCH FROM f.finished_at - c.started_at)
FROM metrics m
CROSS JOIN context c
CROSS JOIN finished f;

COMMIT;
