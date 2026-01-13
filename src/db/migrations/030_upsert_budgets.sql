BEGIN;

WITH deduped AS (
    SELECT *
    FROM (
        SELECT
            sb.*,
            ROW_NUMBER() OVER (
                PARTITION BY sb.budget_id
                ORDER BY sb.ingested_at DESC NULLS LAST
            ) AS rn
        FROM stg_budgets sb
    ) t
    WHERE t.rn = 1
)

INSERT INTO budgets (
    budget_id,
    budget_uuid,
    name,
    description,
    retired,
    start_date,
    recurring_interval,
    timezone,
    limit_amount,
    overspend_buffer,
    assigned_amount,
    spent_cleared,
    spent_pending,
    spent_total,
    sync_day,
    created_at,
    updated_at,
    source
)
SELECT
    d.budget_id,
    d.budget_uuid,
    d.name,
    d.description,
    d.retired,
    d.start_date,
    d.recurring_interval,
    d.timezone,
    COALESCE(d.limit_amount, 0) AS limit_amount,
    d.overspend_buffer,
    d.assigned_amount,
    d.spent_cleared,
    d.spent_pending,
    d.spent_total,
    d.sync_day,
    NOW() AS created_at,
    NOW() AS updated_at,
    d.source
FROM deduped d

ON CONFLICT (budget_id)
DO UPDATE SET
    budget_uuid       = EXCLUDED.budget_uuid,
    name              = EXCLUDED.name,
    description       = EXCLUDED.description,
    retired           = EXCLUDED.retired,
    start_date        = EXCLUDED.start_date,
    recurring_interval= EXCLUDED.recurring_interval,
    timezone          = EXCLUDED.timezone,
    limit_amount      = EXCLUDED.limit_amount,
    overspend_buffer  = EXCLUDED.overspend_buffer,
    assigned_amount   = EXCLUDED.assigned_amount,
    spent_cleared     = EXCLUDED.spent_cleared,
    spent_pending     = EXCLUDED.spent_pending,
    spent_total       = EXCLUDED.spent_total,
    sync_day          = EXCLUDED.sync_day,
    updated_at        = NOW(),
    source            = EXCLUDED.source;

COMMIT;
