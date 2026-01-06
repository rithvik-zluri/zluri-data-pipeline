Schema Design Explanation

Users
The users table stores all user entities with a unique identifier.
A status field is used to track whether a user is active or inactive based on the given logic.

Groups
The groups table supports hierarchical relationships using parent_group_id.
This enables representation of nested groups such as Organization -> Department -> Team.

User-Group Relationship
A many-to-many relationship exists between users and groups.
This is implemented using the user_group_membership mapping table.

This design allows:

    One user to belong to multiple groups
    One group to contain multiple users

Transactions
The transactions table stores all financial events linked to users.
It includes:

    original currency
    converted USD amount
    processing timestamp for incremental loading

This structure supports:

    deduplication
    incremental ingestion
    analytical reporting

User Inactive Logic
A user is marked as inactive if:
    They are not present in the latest Day 1 ingestion
    They are not present in the Day 2 ingestion

This ensures that users removed from the source are correctly reflected as inactive.

Group Inactive Logic
A group is marked as inactive if:
    It contains zero active users
    All of its child groups are inactive

This requires recursive evaluation of the group hierarchy.