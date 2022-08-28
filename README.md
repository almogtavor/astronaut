<img align="right" width="220" height="220" src="astronaut.png">


# Astronaut

A project that enables the option of sending data out of the system by using SQL as a complex event processing tool.
The program also has the abilities of debugging a query and specifying exactly why haven't it succeeded.

## Queries Debugging

When the data is for example:

|  name  | age | job |
|:------:|:---:|:---:|
| George | 30|Developer|

And the required task is to investigate why did this row fail, <br>
so for the SQL statement of
```sql
SELECT * FROM people WHERE name="Ou" AND (age<31 OR age>50)
```
We'd expect the result of the debugging process to be `name="Ou"`, since even though `age>50` failed either, 
the `age<31 OR age>50` statement returned `true`.

In a different example, the query:
```sql
SELECT * FROM people WHERE name="Ou" AND (age<31 OR age>50) AND job="Officer"
```
should result a debugging process that will return `name="Ou"` and `job="Officer"` (these sub-SQL-statement is called a leaf statement), 
since both of them have caused the query to fail. 

Currently, it’s done by recursively iterating all child nodes of the query by the `df.queryExecution.analyzed()` method.
For each node I run it as a `WHERE` clause (e.g. `SELECT * FROM people WHERE name=George`) and I count the returned records to check if that’s passes or not.
`1` means that the query passes, `0` means failure.

### Configuration Options

The debugging process can be configured as following:

`debug-mode`:

A configuration for what should the executor of the debugging process debug.
When specifying `ON_FAILURE`, the program will try to answer the question of "Why did this records failed?" (hence it'll be possible to run this only for failed records).
`ON_SUCCESS` is the exact opposite. 

| Option |             Explanation              |
|:------:|:------------------------------------:|
| ON_FAILURE | Enable debugging for failed records  |
| ON_SUCCESS | Enable debugging for success records |
| NEVER |     Disables the debugging task      |

`debug-depth-level`:

A configuration for what depth should the executor search for at the SQL tree node.
When specifying `LEAF`, 
`HIGHEST_PROBLEMATIC_NODE` . 

| Option |             Explanation              |
|:------:|:------------------------------------:|
| LEAF |   |
| HIGHEST_PROBLEMATIC_NODE |  |
