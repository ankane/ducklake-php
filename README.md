# DuckLake PHP

:duck: [DuckLake](https://ducklake.select/) for PHP

Run your own data lake with a SQL database and file/object storage

```php
new DuckLake\Client(
    catalogUrl: 'postgres://user:pass@host:5432/dbname',
    storageUrl: 's3://my-bucket/'
);
```

[Learn more](https://duckdb.org/2025/05/27/ducklake.html)

Note: DuckLake is [not considered production-ready](https://ducklake.select/faq#is-ducklake-production-ready) at the moment

[![Build Status](https://github.com/ankane/ducklake-php/actions/workflows/build.yml/badge.svg)](https://github.com/ankane/ducklake-php/actions)

## Installation

Run:

```sh
composer require ankane/ducklake
```

## Getting Started

Create a client - this one stores everything locally

```php
$ducklake = new DuckLake\Client(
    catalogUrl: 'sqlite:///ducklake.sqlite',
    storageUrl: 'data_files/',
    createIfNotExists: true
);
```

Create a table

```php
$ducklake->sql('CREATE TABLE events (id bigint, name text)');
```

Load data from a file

```php
$ducklake->sql("COPY events FROM 'data.csv'");
```

Confirm a new Parquet file was added to the data lake

```php
$ducklake->listFiles('events');
```

Query the data

```php
$ducklake->sql('SELECT COUNT(*) FROM events')->toArray();
```

## Catalog Database

Catalog information can be stored in:

- Postgres: `postgres://user@pass@host:5432/dbname`
- SQLite: `sqlite:///path/to/dbname.sqlite`
- DuckDB: `duckdb:///path/to/dbname.duckdb`

Note: MySQL and MariaDB are not currently supported due to [duckdb/ducklake#70](https://github.com/duckdb/ducklake/issues/70) and [duckdb/ducklake#210](https://github.com/duckdb/ducklake/issues/210)

There are two ways to set up the schema:

1. Run [this script](https://ducklake.select/docs/stable/specification/tables/overview#full-schema-creation-script)
2. Configure the client to do it

  ```php
  new DuckLake\Client(createIfNotExists: true, ...)
  ```

## Data Storage

Data can be stored in:

- Local files: `data_files/`
- Amazon S3: `s3://my-bucket/path/`
- [Other providers](https://ducklake.select/docs/stable/duckdb/usage/choosing_storage): todo

### Amazon S3

Credentials are detected in the standard AWS SDK locations

IAM permissions

- Read: `s3::ListBucket`, `s3::GetObject`
- Write: `s3::ListBucket`, `s3::PutObject`
- Maintenance: `s3::ListBucket`, `s3::GetObject`, `s3::PutObject`, `s3::DeleteObject`

## Operations

Create an empty table

```php
$ducklake->sql('CREATE TABLE events (id bigint, name text)');
```

Or a table from a file

```php
$ducklake->sql("CREATE TABLE events AS FROM 'data.csv'");
```

Load data from a file

```php
$ducklake->sql("COPY events FROM 'data.csv'");
```

You can also load data directly from other [data sources](https://duckdb.org/docs/stable/data/data_sources)

```php
$ducklake->attach('blog', 'postgres://localhost:5432/blog');
$ducklake->sql('INSERT INTO events SELECT * FROM blog.events');
```

Or [register existing data files](https://ducklake.select/docs/stable/duckdb/metadata/adding_files)

```php
$ducklake->addDataFiles('events', 'data.parquet');
```

Note: This transfers ownership to the data lake, so the file may be deleted as part of [maintenance](#maintenance)

Update data

```php
$ducklake->sql('UPDATE events SET name = ? WHERE id = ?', ['Test', 1]);
```

Delete data

```php
$ducklake->sql('DELETE * FROM events WHERE id = ?', [1]);
```

## Schema Changes

Update the schema

```php
$ducklake->sql('ALTER TABLE events ADD COLUMN active BOOLEAN');
```

Set or remove a [partitioning key](https://ducklake.select/docs/stable/duckdb/advanced_features/partitioning)

```php
$ducklake->sql('ALTER TABLE events SET PARTITIONED BY (name)');
// or
$ducklake->sql('ALTER TABLE events RESET PARTITIONED BY');
```

## Views

Create a view

```php
$ducklake->sql('CREATE VIEW events_view AS SELECT * FROM events');
```

Drop a view

```php
$ducklake->sql('DROP VIEW events_view');
```

## Snapshots

Get snapshots

```php
$ducklake->snapshots();
```

Query the data at a specific snapshot version or time

```php
$ducklake->sql('SELECT * FROM events AT (VERSION => ?)', [3]);
# or
$ducklake->sql('SELECT * FROM events AT (TIMESTAMP => ?)', [new DateTime()]);
```

You can also specify a snapshot when creating the client

```php
new DuckLake\Client(snapshotVersion: 3, ...);
// or
new DuckLake\Client(snapshotTime: new DateTime(), ...);
```

## Maintenance

Merge files

```php
$ducklake->mergeAdjacentFiles();
```

Expire snapshots

```php
$ducklake->expireSnapshots(olderThan: new DateTime());
```

Clean up old files

```php
$ducklake->cleanupOldFiles(olderThan: new DateTime());
```

Rewrite files with a certain percentage of deleted rows

```php
$ducklake->rewriteDataFiles(deleteThreshold: 0.5);
```

## Configuration

Get [options](https://ducklake.select/docs/stable/duckdb/usage/configuration)

```php
$ducklake->options();
```

Set an option globally

```php
$ducklake->setOption('parquet_compression', 'zstd');
```

Or for a specific table

```php
$ducklake->setOption('parquet_compression', 'zstd', tableName: 'events');
```

## SQL Safety

Use parameterized queries when possible

```php
$ducklake->sql('SELECT * FROM events WHERE id = ?', [1]);
```

For places that do not support parameters, use `quote` or `quoteIdentifier`

```php
$quotedTable = $ducklake->quoteIdentifier('events');
$quotedFile = $ducklake->quote('path/to/data.csv');
$ducklake->sql("COPY $quotedTable FROM $quotedFile");
```

## Reference

Get table info

```php
$ducklake->tableInfo();
```

Drop a table

```php
$ducklake->dropTable('events');
# or
$ducklake->dropTable('events', ifExists: true);
```

List files

```php
$ducklake->listFiles('events');
```

List files at a specific snapshot version or time

```php
$ducklake->listFiles('events', snapshotVersion: 3);
# or
$ducklake->listFiles('events', snapshotTime: new DateTime());
```

## History

View the [changelog](https://github.com/ankane/ducklake-php/blob/master/CHANGELOG.md)

## Contributing

Everyone is encouraged to help improve this project. Here are a few ways you can help:

- [Report bugs](https://github.com/ankane/ducklake-php/issues)
- Fix bugs and [submit pull requests](https://github.com/ankane/ducklake-php/pulls)
- Write, clarify, or fix documentation
- Suggest or add new features

To get started with development:

```sh
git clone https://github.com/ankane/ducklake-php.git
cd ducklake-php
composer install

# Postgres
createdb ducklake_php_test
createdb ducklake_php_test2
CATALOG=postgres composer test

# MySQL and MariaDB
mysqladmin create ducklake_php_test
mysqladmin create ducklake_php_test2
CATALOG=mysql composer test

# SQLite
CATALOG=sqlite composer test

# DuckDB
CATALOG=duckdb composer test
```
