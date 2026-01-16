BEGIN;

TRUNCATE TABLE
    agent_role_mapping,
    agent_role_history,
    agent_availability,
    agent_status_history,
    agent_pipeline_errors,
    agent_role_pipeline_errors,

    group_membership,
    group_pipeline_errors,

    role_pipeline_errors,

    budget_pipeline_errors,
    card_pipeline_errors,
    transaction_pipeline_errors,

    transaction_pipeline_state,

    stg_agents,
    stg_agent_details,
    stg_agent_availability,
    stg_groups,
    stg_group_membership,
    stg_roles,
    stg_agent_roles,
    stg_agent_role_mapping,
    stg_budgets,
    stg_cards,
    stg_transactions,

    transactions,
    cards,
    budgets,

    agent_details,
    agents,

    roles,

    groups;

COMMIT;
