Schema Design

The schema is designed using PostgreSQL and follows normalization principles to avoid duplication and maintain data integrity.

Entities and Tables

1. Agents

Source:  
- sync-day1/agents  
- sync-day2/agents  
- sync-day1/agent_details  
- sync-day2/agent_details  

Description:  
The `agents` table stores core information about support agents such as name, email, availability, and operational status.

Primary Key: `agent_id`

Each agent represents a unique support user in the organization.

2. Agent Details

Source: 
- sync-day1/agent_details  
- sync-day2/agent_details  

Description:  
The `agent_details` table stores extended and nested information about agents such as contact info, availability, and ticket scope.

This table is separated from `agents` to keep the main table clean and normalized.

Relationship:
One-to-one with `agents` using `agent_id`.

3. Admin Groups

Source:
- sync-day1/admin_groups  
- sync-day2/admin_groups  

Description: 
The `admin_groups` table stores organizational groups such as DevOps, IT Support, Hardware Procurement, etc.

Each group contains multiple agents.

Primary Key: `group_id`

4. Agent Group Membership

Source:
- agent_ids array inside admin_groups JSON  

Description:
Since each group contains an array of agent IDs, a separate mapping table `agent_group_membership` is used to model the many-to-many relationship between agents and groups.

Primary Keys: (`agent_id`, `group_id`)

5. Roles

Source: 
- sync-day1/roles  
- sync-day2/roles  

Description:
The `roles` table stores role definitions and permissions assigned to agents.

Each role represents a specific access level or responsibility.

6. Budgets

Source:
- sync-day1/budgets  
- sync-day2/budgets  
- tran/budgets  

Description: 
The `budgets` table stores financial budget allocations.

Budgets can come from both sync and transactional domains.

A `source` column is used to identify the origin of the data.

7. Cards

Source:  
- sync-day1/cards  
- sync-day2/cards  
- tran/cards  

Description:
The `cards` table stores corporate card information issued to agents.

Each card is linked to an agent.

8. Transactions

Source:
- sync-day1/transactions  
- sync-day2/transactions  
- tran/transactions  

Description:
The `transactions` table stores financial transactions performed using corporate cards.

Each transaction is linked to both:
- an agent
- a card

Design Decisions

Why PostgreSQL?
PostgreSQL is chosen because:
- Strong support for relational data
- Excellent handling of structured data
- Supports JSONB for semi-structured fields
- Widely used in data engineering pipelines

Why Separate Tables?
Data is separated into multiple tables to:
- Avoid duplication
- Improve data integrity
- Support scalable joins and analytics

Why Mapping Table for Groups?
Because one agent can belong to multiple groups and one group can have multiple agents, a many-to-many relationship exists.  
This is handled using the `agent_group_membership` table.

Why `source` Column?
The `source` column helps track whether data came from:
- sync-day1
- sync-day2
- tran

This enables:
- historical comparisons
- incremental processing
- debugging

Summary

This schema is designed to:
- Cleanly represent all provided JSON data
- Support scalable ETL pipelines
- Maintain relational integrity
- Handle future growth in data volume
