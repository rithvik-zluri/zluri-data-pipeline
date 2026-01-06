Database Technology Decision
Chosen Approach: PostgreSQL as primary database
The core entities in this project — Users, Groups, and Transactions — have strong relational characteristics:

Users can belong to multiple groups (many-to-many)

Groups have hierarchical parent–child relationships

Transactions are linked to users and require deduplication, incremental loading, and analytical querying

Given these requirements, a relational database is the most appropriate choice.

PostgreSQL was selected because:

It provides strong referential integrity using foreign keys

It supports complex joins required for group hierarchy resolution.

It enables efficient analytical queries on transaction data

It simplifies inactive logic implementation through SQL aggregation

Optional NoSQL Usage
MongoDB can be optionally used for:

Storing raw ingestion data

Logging pipeline execution metadata

Storing semi-structured API responses

However, MongoDB is not used as the primary datastore due to the relational nature of the data.
..