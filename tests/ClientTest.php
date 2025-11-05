<?php

use Tests\TestCase;

final class ClientTest extends TestCase
{
    public function testSnapshots()
    {
        $this->clearSnapshots();

        $this->assertCount(1, $this->client->snapshots());
        $this->createEvents();
        $this->assertCount(2, $this->client->snapshots());
        $this->loadEvents();
        $this->assertCount(3, $this->client->snapshots());
    }

    public function testSchemaEvolution()
    {
        $this->createEvents();
        $this->client->sql("ALTER TABLE events ADD COLUMN c VARCHAR DEFAULT 'hello'");
        $result = $this->client->sql('SELECT * FROM events');
        $this->assertEquals(['a', 'b', 'c'], $result->columns());
        $this->assertEquals('hello', $result->toArray()[0]['c']);
    }

    public function testOptions()
    {
        $this->assertIsArray($this->client->options());
    }

    // note: ducklake_set_option creates duplicate entries
    public function testSetOptionGlobal()
    {
        $this->client->setOption('parquet_compression', 'snappy');
        $option = array_find($this->client->options(), fn ($v) => $v['option_name'] == 'parquet_compression' && $v['scope'] == 'GLOBAL');
        $this->assertEquals('snappy', $option['value']);
    }

    // note: ducklake_set_option creates duplicate entries
    public function testSetOptionTable()
    {
        $this->createEvents();
        $this->client->setOption('parquet_compression', 'zstd', tableName: 'events');
        $option = array_find($this->client->options(), fn ($v) => $v['option_name'] == 'parquet_compression' && $v['scope'] == 'TABLE' && $v['scope_entry'] == 'main.events');
        $this->assertEquals('zstd', $option['value']);
    }

    public function testFormatVersion()
    {
        $this->assertEquals('0.2', $this->client->formatVersion());
    }

    public function testExtensionVersion()
    {
        $this->assertEquals('9cc2d90', $this->client->extensionVersion());
    }

    public function testDuckdbVersion()
    {
        $this->assertEquals('v1.3.2', $this->client->duckdbVersion());
    }

    public function testMergeAdjacentFiles()
    {
        $this->createEvents();
        $this->loadEvents();
        $this->assertCount(2, $this->client->listFiles('events'));

        $this->client->mergeAdjacentFiles();
        $this->assertCount(1, $this->client->listFiles('events'));
    }

    public function testExpireSnapshots()
    {
        $this->clearSnapshots();

        $this->assertCount(0, $this->client->expireSnapshots(olderThan: new DateTime()));
        $this->assertCount(1, $this->client->snapshots());

        $this->createEvents();
        $this->assertCount(2, $this->client->snapshots());

        $this->assertCount(1, $this->client->expireSnapshots(olderThan: new DateTime(), dryRun: true));
        $this->assertCount(2, $this->client->snapshots());

        $this->assertCount(1, $this->client->expireSnapshots(olderThan: new DateTime()));
        $this->assertCount(1, $this->client->snapshots());
    }

    public function testCleanupOldFiles()
    {
        $this->clearOldFiles();

        $this->createEvents();
        $this->client->dropTable('events');

        $this->assertCount(0, $this->client->cleanupOldFiles(cleanupAll: true, dryRun: true));

        $this->client->expireSnapshots(olderThan: new DateTime());
        $this->assertCount(1, $this->client->cleanupOldFiles(cleanupAll: true, dryRun: true));
        $this->assertCount(1, $this->client->cleanupOldFiles(cleanupAll: true));
    }

    public function testListFiles()
    {
        $this->clearOldFiles();

        $this->createEvents();
        $this->assertCount(1, $this->client->listFiles('events'));

        $snapshots = $this->client->snapshots();
        $snapshot = end($snapshots);

        $this->loadEvents();
        $this->assertCount(2, $this->client->listFiles('events'));

        $this->assertCount(1, $this->client->listFiles('events', snapshotVersion: $snapshot['snapshot_id']));
    }

    public function testAddDataFiles()
    {
        $this->createEvents();
        $this->assertCount(1, $this->client->listFiles('events'));
        $this->assertCount(3, $this->client->sql('SELECT * FROM events')->rows());

        // note: add_data_files transfers ownership to DuckLake
        // which can delete the files
        // https://ducklake.select/docs/stable/duckdb/metadata/adding_files
        copy('tests/support/data.parquet', $this->tmpdir() . '/data.parquet');
        $this->client->addDataFiles('events', $this->tmpdir() . '/data.parquet');
        $this->assertCount(2, $this->client->listFiles('events'));
        $this->assertCount(6, $this->client->sql('SELECT * FROM events')->toArray());
    }

    public function testAddDataFilesMissing()
    {
        $this->expectException(Saturio\DuckDB\Exception\PreparedStatementExecuteException::class);
        $this->expectExceptionMessage('No files found that match the pattern');

        $this->createEvents();
        $this->client->addDataFiles('events', 'tests/support/missing.parquet');
    }

    public function testAddDataFilesInvalid()
    {
        $this->expectException(Saturio\DuckDB\Exception\PreparedStatementExecuteException::class);
        $this->expectExceptionMessage('No magic bytes found');

        $this->createEvents();
        $this->client->addDataFiles('events', 'tests/support/data.csv');
    }

    public function testDataInlining()
    {
        $this->markTestSkipped('Requires DuckDB 1.4+');

        $client = $this->newClient(
            catalogUrl: 'duckdb:///' . $this->tmpdir() . '/inlined.duckdb',
            dataInliningRowLimit: 10,
            createIfNotExists: true
        );
        $client->sql('DROP TABLE IF EXISTS events');
        $client->sql("CREATE TABLE events AS FROM 'tests/support/data.csv'");
        $this->assertCount(0, $client->listFiles('events'));

        $this->client->flushInlinedData();
        $this->assertCount(1, $client->listFiles('events'));
    }

    public function testTableInfo()
    {
        $this->createEvents();
        $info = $this->client->tableInfo();
        $this->assertCount(1, $info);
        $this->assertEquals('events', $info[0]['table_name']);
    }

    public function testDropTable()
    {
        $this->expectNotToPerformAssertions();

        $this->createEvents();
        $this->client->dropTable('events');
    }

    public function testDropTableMissing()
    {
        // TODO fix
        $this->expectException(TypeError::class);

        $this->client->dropTable('events');
    }

    public function testDropTableIfExists()
    {
        $this->expectNotToPerformAssertions();

        $this->createEvents();
        $this->client->dropTable('events', ifExists: true);
        $this->client->dropTable('events', ifExists: true);
    }

    public function testAttachPostgres()
    {
        $pg = pg_connect('dbname=ducklake_php_test');
        pg_query($pg, 'DROP TABLE IF EXISTS postgres_events');
        pg_query($pg, 'CREATE TABLE postgres_events (id bigint, name text)');
        pg_query_params($pg, 'INSERT INTO postgres_events VALUES ($1, $2)', [1, 'Test']);

        $this->client->attach('pg', 'postgres://localhost/ducklake_php_test');

        $this->client->sql('CREATE TABLE events (id bigint, name text)');
        $this->client->sql('INSERT INTO events SELECT * FROM pg.postgres_events');

        $expected = [['id' => 1, 'name' => 'Test']];
        $this->assertEquals($expected, $this->client->sql('SELECT * FROM events')->toArray());

        $this->client->detach('pg');

        // TODO fix
        $this->expectException(TypeError::class);
        $this->client->sql('INSERT INTO events SELECT * FROM pg.postgres_events');
    }

    public function testAttachPostgresReadOnly()
    {
        $this->expectException(Saturio\DuckDB\Exception\PreparedStatementExecuteException::class);
        $this->expectExceptionMessage('attached in read-only mode!');

        $pg = pg_connect('dbname=ducklake_php_test');
        pg_query($pg, 'DROP TABLE IF EXISTS postgres_events');
        pg_query($pg, 'CREATE TABLE postgres_events (id bigint, name text)');

        $this->client->attach('pg', 'postgres://localhost/ducklake_php_test');

        $this->client->sql('INSERT INTO pg.postgres_events VALUES (?, ?)', [2, 'Test 2']);
    }

    public function testAttachUnsupportedType()
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unsupported data source type');

        $this->client->attach('hello', 'pg://');
    }
}
