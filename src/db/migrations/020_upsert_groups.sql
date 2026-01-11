-- =========================
-- UPSERT GROUPS
-- =========================
INSERT INTO groups (group_id, name, parent_group_id, created_at, updated_at)
SELECT group_id, name, parent_group_id, created_at, updated_at
FROM stg_groups
ON CONFLICT (group_id)
DO UPDATE SET
    name = EXCLUDED.name,
    parent_group_id = EXCLUDED.parent_group_id,
    updated_at = EXCLUDED.updated_at;

-- =========================
-- REFRESH MEMBERSHIP
-- =========================
DELETE FROM group_membership
WHERE group_id IN (SELECT DISTINCT group_id FROM stg_group_membership);


INSERT INTO group_membership (group_id, agent_id)
SELECT group_id, agent_id
FROM stg_group_membership;

-- =========================
-- RECURSIVE INACTIVE LOGIC
-- =========================
WITH RECURSIVE group_tree AS (
    -- base: each group
    SELECT 
        g.group_id,
        g.parent_group_id,
        g.group_id AS root_group_id
    FROM groups g

    UNION ALL

    -- recursive: walk up the tree
    SELECT 
        parent.group_id,
        parent.parent_group_id,
        child.root_group_id
    FROM groups parent
    JOIN group_tree child
        ON parent.group_id = child.parent_group_id
),

active_groups AS (
    SELECT DISTINCT group_id
    FROM group_membership gm
    JOIN agents a ON gm.agent_id = a.agent_id
    WHERE a.deactivated = false
),

groups_with_active_descendants AS (
    SELECT DISTINCT root_group_id AS group_id
    FROM group_tree gt
    JOIN active_groups ag ON gt.group_id = ag.group_id
)

UPDATE groups
SET inactive = CASE
    WHEN group_id IN (SELECT group_id FROM groups_with_active_descendants)
         THEN false
    ELSE true
END;
