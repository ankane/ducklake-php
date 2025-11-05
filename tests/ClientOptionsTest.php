<?php

use Tests\TestCase;

final class ClientOptionsTest extends TestCase
{
    public function testCatalogUrlInvalidUrl()
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unsupported catalog type');

        $this->newClient(catalogUrl: 'invalid url');
    }

    public function testCatalogUrlUnsupportedType()
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unsupported catalog type');

        $this->newClient(catalogUrl: 'pg://localhost');
    }

    public function testSnapshotVersion()
    {
        $this->expectException(Saturio\DuckDB\Exception\PreparedStatementExecuteException::class);
        $this->expectExceptionMessage('No snapshot found at version 1000000000');

        $this->newClient(snapshotVersion: 1000000000);
    }

    public function testSnapshotTime()
    {
        $this->expectNotToPerformAssertions();

        $this->newClient(snapshotTime: new DateTime());
    }

    public function testSnapshotVersionSnapshotTime()
    {
        $this->expectException(Saturio\DuckDB\Exception\PreparedStatementExecuteException::class);
        $this->expectExceptionMessage('Cannot specify both VERSION and TIMESTAMP');

        $this->newClient(snapshotVersion: 1, snapshotTime: new DateTime());
    }

    public function testCreateIfNotExists()
    {
        $this->expectException(Saturio\DuckDB\Exception\PreparedStatementExecuteException::class);
        $this->expectExceptionMessage('creating a new DuckLake is explicitly disabled');

        $this->newClient(catalogUrl: 'sqlite:///' . $this->tmpdir() . '/empty.sqlite');
    }
}
