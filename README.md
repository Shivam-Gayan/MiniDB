# SimpleDB Engine

**SimpleDB** is a lightweight, disk-based relational database engine built entirely from scratch in C#. It demonstrates core database internals including paged storage architecture, B+ Tree indexing, a custom SQL parser, and a query execution engine.


## Key Features

* **Disk-Based Persistence**: Uses a custom paged file format with **4KB pages** to store data persistently on disk.
* **B+ Tree Indexing**: Implements a robust B+ Tree for efficient `O(log n)` lookups, range scans, and sorted traversals.
* **Custom SQL Parser**: Features a hand-written **Lexer** and **Recursive Descent Parser** supporting a subset of SQL.
* **Storage Architecture**: Uses a **Slotted Page** structure to manage variable-length records and fragmentation.
* **Interactive REPL**: A command-line interface for managing databases and executing queries in real-time.
* **Space Management**: Includes a `VACUUM` command to compact pages and reclaim space from deleted records.

---

## Supported Data Types

| Type | Description |
| :--- | :--- |
| `INT` / `INTEGER` | 32-bit signed integer. |
| `DOUBLE` | 64-bit floating point number. |
| `BOOL` / `BOOLEAN` | True (`true`) or False (`false`) value. |
| `STRING` | UTF-8 variable-length text string (must be single-quoted). |



---

##  Command Reference

> **⚠️ Note:** The engine **does not support multiline commands**. You must type the entire statement on a single line before pressing Enter.

### 1. Session & Meta Commands
These commands are handled directly by the engine shell and bypass the SQL parser. They are case-insensitive.

**Database Management**
```sql
CREATE DATABASE my_db      -- Create a new database file
USE my_db                  -- Switch active context to this database
DROP DATABASE my_db        -- Delete a database file

```

**Information Schema**

```sql
SHOW DATABASES             -- List all database files
SHOW TABLES                -- List tables in current DB
SHOW INDEXES               -- List all indexes

```

**Maintenance & System**

```sql
VACUUM                     -- Reclaim free space in current DB
EXIT                       -- Close the application

```

### 2. Data Definition Language (DDL)

**Create Table**
Defines a new table schema.

```sql
CREATE TABLE users (
    id INT,
    name STRING,
    age INT,
    is_active BOOL
);

```

**Drop Table**
Removes a table and all its data.

```sql
DROP TABLE users;

```

**Create Index**
Builds a B+ Tree index on a specific column for faster lookups.

```sql
CREATE INDEX idx_age ON users(age);

```

**Drop Index**
Removes an index. (Note the syntax uses `table.column`)

```sql
DROP INDEX users.age;

```

### 3. Data Manipulation Language (DML)

**Insert Data**
Adds a new record. You must provide values for all columns in the exact order defined in the schema.

```sql
INSERT INTO users VALUES (1, 'Alice', 30, true);

```

**Select Data**
Retrieves data. Supports `WHERE` filtering with operators: `=`, `<`, `>`, `<=`, `>=`, and `BETWEEN`.

```sql
-- Select all
SELECT * FROM users;

-- Exact match (uses Index if available)
SELECT * FROM users WHERE id = 1;

-- Range scan
SELECT * FROM users WHERE age > 25;
SELECT * FROM users WHERE age BETWEEN 20 AND 30;

```

**Update Data**
Modifies specific columns for rows matching the condition.

```sql
UPDATE users SET is_active = false WHERE id = 1;

```

**Delete Data**
Removes rows matching the condition.

```sql
DELETE FROM users WHERE age < 18;

```

---

## Architecture Overview

The engine is layered into distinct components:

1. **Session Layer**: Handles REPL interaction and meta-commands.
2. **Parsing Layer**: Tokenizes raw text (`Lexer`) and builds an Abstract Syntax Tree (`SqlParser`).
3. **Execution Layer**: The `CommandBuilder` converts AST nodes into executable Commands (`SelectCommand`, `InsertCommand`, etc.).
4. **Access Methods**:
* **Table Scan**: Iterates through heap pages for non-indexed queries.
* **Index Search**: Uses B+ Trees to find Record IDs (RIDs) efficiently.


5. **Storage Manager**: Manages file I/O, page allocation, and buffer reading/writing.

## Getting Started

1. **Build**: Open the solution in Visual Studio or run `dotnet build`.
2. **Run**: Start the console application.
3. **Example Session**:
```bash
> create database shop
> use shop
shop> create table items(id int, name string, price double)
shop> insert into items values(1, 'Apple', 0.99)
shop> insert into items values(2, 'Orange', 1.25)
shop> select * from items where price > 1.00

```





