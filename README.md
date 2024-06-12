# Obsolete Indexes in SQL Server


This repository contains SQL Server scripts to identify and manage obsolete indexes in a database. Obsolete indexes include duplicate indexes and columns repeated in more than one index. The scripts provided here offer a solution to track these indexes and dynamically generate commands to remove them.

## Repository Structure

- `repeated_indexes.sql`: Script to identify obsolete indexes and save them in a global temporary table.
- `dinamic_query_drop.sql`: Script to dynamically generate commands to remove the identified obsolete indexes.

## Requirements

- SQL Server 2012 or later
- Sufficient permissions to create and manipulate temporary tables and indexes

## How to Use

### Step 1: Track Obsolete Indexes

1. Open the `repeated_indexes.sql` file.
2. Connect to the database where you want to track the indexes.
3. Execute the script.
4. Review the indexes identified for deletion.

This script will:

- Identify duplicate indexes and columns repeated in indexes.
- Return the data in a select statement for review.
- Store this information in a global temporary table called  `##buscar_indices`.

### Step 2: Remove Obsolete Indexes

1. Open the  `dinamic_query_drop.sql` file.
2. Connect to the same database where the previous script was executed.
3. Execute the script.

This script will:

- Read the data from the global temporary table `##buscar_indices`.
- Dynamically generate SQL commands to check the existence of each obsolete index and, if they exist, remove them.

```sql
if exists
                (
                   select  top 1 1
                   from    sys.objects ob
                   join    sys.schemas sc
                   on      ob.schema_id = sc.schema_id
                   join    sys.indexes id
                   on      ob.object_id = id.object_id
                   where   ob.name = 'tItemMovimento'
                   and     sc.name = 'dbo'
                   and     id.name = 'idxiIDMovimento'
                )
    begin
        drop index [idxiIDMovimento] on dbo.tItemMovimento
    end```