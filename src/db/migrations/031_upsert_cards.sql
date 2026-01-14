BEGIN;

WITH deduped AS (
    SELECT DISTINCT ON (card_id)
        card_id,
        card_uuid,
        name,
        user_id,
        user_uuid,
        budget_id,
        budget_uuid,
        last_four,
        valid_thru,
        status,
        type,
        share_budget_funds,
        recurring,
        recurring_limit,
        current_limit,
        current_spent,
        created_time,
        updated_time,
        sync_day AS last_synced_day,
        source,
        ingested_at
    FROM stg_cards
    ORDER BY card_id, ingested_at DESC
)

INSERT INTO cards (
    card_id,
    card_uuid,
    name,
    user_id,
    user_uuid,
    budget_id,
    budget_uuid,
    last_four,
    valid_thru,
    status,
    type,
    share_budget_funds,
    recurring,
    recurring_limit,
    current_limit,
    current_spent,
    created_time,
    updated_time,
    last_synced_day,
    source,
    ingested_at
)
SELECT
    card_id,
    card_uuid,
    name,
    user_id,
    user_uuid,
    budget_id,
    budget_uuid,
    last_four,
    valid_thru,
    status,
    type,
    share_budget_funds,
    recurring,
    recurring_limit,
    current_limit,
    current_spent,
    created_time,
    updated_time,
    last_synced_day,
    source,
    ingested_at
FROM deduped
ON CONFLICT (card_id) DO UPDATE
SET
    card_uuid = EXCLUDED.card_uuid,
    name = EXCLUDED.name,
    user_id = EXCLUDED.user_id,
    user_uuid = EXCLUDED.user_uuid,
    budget_id = EXCLUDED.budget_id,
    budget_uuid = EXCLUDED.budget_uuid,
    last_four = EXCLUDED.last_four,
    valid_thru = EXCLUDED.valid_thru,
    status = EXCLUDED.status,
    type = EXCLUDED.type,
    share_budget_funds = EXCLUDED.share_budget_funds,
    recurring = EXCLUDED.recurring,
    recurring_limit = EXCLUDED.recurring_limit,
    current_limit = EXCLUDED.current_limit,
    current_spent = EXCLUDED.current_spent,
    created_time = EXCLUDED.created_time,
    updated_time = EXCLUDED.updated_time,
    last_synced_day = EXCLUDED.last_synced_day,
    source = EXCLUDED.source,
    ingested_at = EXCLUDED.ingested_at;

COMMIT;
