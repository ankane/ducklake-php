<?php

namespace Tests;

use PHPUnit\Framework\TestCase as BaseTestCase;

abstract class TestCase extends BaseTestCase
{
    protected $client;

    protected function setUp(): void
    {
        $this->client = $this->newClient(createIfNotExists: true);

        $this->client->dropTable('events', ifExists: true);
    }

    protected function storageUrl()
    {
        return getenv('STORAGE_URL') ?: ($this->tmpdir() . '/data_files');
    }

    protected function catalogUrl()
    {
        $catalog = $this->catalog();
        if ($catalog == 'postgres') {
            return 'postgres://localhost/ducklake_php_test';
        } elseif ($catalog == 'mysql') {
            return 'mysql://localhost/ducklake_php_test';
        } elseif ($catalog == 'mariadb') {
            return 'mariadb://localhost/ducklake_php_test';
        } elseif ($catalog == 'sqlite') {
            return 'sqlite:///' . $this->tmpdir() . '/ducklake_php_test.sqlite';
        } elseif ($catalog == 'duckdb') {
            return 'duckdb:///' . $this->tmpdir() . '/ducklake_php_test.duckdb';
        } else {
            throw new \Exception('Unsupported catalog');
        }
    }

    protected function catalog()
    {
        return getenv('CATALOG') ?: 'postgres';
    }

    protected function clientOptions()
    {
        return [
            'catalogUrl' => $this->catalogUrl(),
            'storageUrl' => $this->storageUrl()
        ];
    }

    protected function newClient(...$options)
    {
        return new \DuckLake\Client(...array_merge($this->clientOptions(), $options));
    }

    protected function tmpdir()
    {
        // TODO improve
        return sys_get_temp_dir();
    }

    protected function createEvents()
    {
        $this->client->sql("CREATE TABLE events AS FROM 'tests/support/data.csv'");
    }

    protected function loadEvents()
    {
        $this->client->sql("COPY events FROM 'tests/support/data.csv'");
    }

    protected function clearSnapshots()
    {
        $this->client->expireSnapshots(olderThan: new \DateTime());
    }

    protected function clearOldFiles()
    {
        $this->clearSnapshots();
        $this->client->cleanupOldFiles(cleanupAll: true);
    }
}
