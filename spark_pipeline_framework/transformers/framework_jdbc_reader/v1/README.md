# USAGE NOTES

When feeding the FrameworkJdbcReader a query, the `query` parameter should be a supplied a query in the form of `"(QUERY) as ALIAS"`.

The reason for the formatting is that we are feeding the query to the Spark sql_ctx (SQL context) option `dbtable` which is looking for a table, not a query. For that reason, the parameter needs to be fed as a subqeury.

For example:
I want to run the query :
```sql
SELECT 
    user_id,
    gender_cd, 
    birth_date
FROM
    member_user_information
WHERE 
    joined_id = 1
```

The query should be fed to the transformer as:

```sql
(SELECT 
    user_id,
    gender_cd, 
    birth_date
FROM
    member_user_information
WHERE 
    joined_id = 1) as my_query
```