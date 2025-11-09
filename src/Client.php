<?php

namespace DuckLake;

class Client
{
    private $catalog;
    private $db;

    public function __construct(
        $catalogUrl,
        $storageUrl,
        $snapshotVersion = null,
        $snapshotTime = null,
        $dataInliningRowLimit = 0,
        $createIfNotExists = false,
        $overrideStorageUrl = false // experimental
    ) {
        $extension = null;
        if (str_starts_with($catalogUrl, 'postgres://') || str_starts_with($catalogUrl, 'postgresql://')) {
            $extension = 'postgres';
            $attach = 'postgres:' . $catalogUrl;
        } elseif (str_starts_with($catalogUrl, 'mysql://') || str_starts_with($catalogUrl, 'mariadb://')) {
            $extension = 'mysql';
            $attach = 'mysql:' . $catalogUrl;
        } elseif (str_starts_with($catalogUrl, 'sqlite:///')) {
            $extension = 'sqlite';
            $attach = 'sqlite:' . substr($catalogUrl, 10);
        } elseif (str_starts_with($catalogUrl, 'duckdb:///')) {
            $attach = 'duckdb:' . substr($catalogUrl, 10);
        } else {
            throw new \InvalidArgumentException('Unsupported catalog type');
        }

        $secretOptions = null;
        if (str_starts_with($storageUrl, 's3://')) {
            $secretOptions = [
                'type' => 's3',
                'provider' => 'credential_chain'
            ];
        }

        $attachOptions = ['data_path' => $storageUrl];
        if (!$createIfNotExists) {
            $attachOptions['create_if_not_exists'] = false;
        }
        if (!is_null($snapshotVersion)) {
            $attachOptions['snapshot_version'] = $snapshotVersion;
        }
        if (!is_null($snapshotTime)) {
            $attachOptions['snapshot_time'] = $snapshotTime;
        }
        if ($dataInliningRowLimit > 0) {
            $attachOptions['data_inlining_row_limit'] = $dataInliningRowLimit;
        }
        if ($overrideStorageUrl) {
            $attachOptions['override_data_path'] = true;
        }

        $this->catalog = 'ducklake';

        $this->db = \Saturio\DuckDB\DuckDB::create();
        $this->installExtension('ducklake');
        if ($extension) {
            $this->installExtension($extension);
        }
        if ($secretOptions) {
            $this->createSecret($secretOptions);
        }
        $this->attachWithOptions($this->catalog, 'ducklake:' . $attach, $attachOptions);
        $this->execute('USE ' . $this->quoteIdentifier($this->catalog));
        $this->detach('memory');
    }

    public function sql($sql, $params = [])
    {
        return $this->execute($sql, $params);
    }

    public function attach($alias, $url)
    {
        $type = null;
        $extension = null;

        if (str_starts_with($url, 'postgres://') || str_starts_with($url, 'postgresql://')) {
            $type = 'postgres';
            $extension = 'postgres';
        } else {
            throw new \InvalidArgumentException('Unsupported data source type');
        }

        if ($extension) {
            $this->installExtension($extension);
        }

        $options = [
            'type' => $type,
            'read_only' => true
        ];
        $this->attachWithOptions($alias, $url, $options);
    }

    public function detach($alias)
    {
        $this->execute('DETACH '. $this->quoteIdentifier($alias));
    }

    public function tableInfo()
    {
        return $this->execute('SELECT * FROM ducklake_table_info(?)', [$this->catalog])->toArray();
    }

    public function dropTable($table, $ifExists = null)
    {
        $this->execute('DROP TABLE ' . ($ifExists ? 'IF EXISTS ' : '') . $this->quoteIdentifier($table));
    }

    // https://ducklake.select/docs/stable/duckdb/usage/snapshots
    public function snapshots()
    {
        return $this->execute('SELECT * FROM ducklake_snapshots(?)', [$this->catalog])->toArray();
    }

    // https://ducklake.select/docs/stable/duckdb/usage/configuration
    public function options()
    {
        return $this->execute('SELECT * FROM ducklake_options(?)', [$this->catalog])->toArray();
    }

    // https://ducklake.select/docs/stable/duckdb/usage/configuration
    public function setOption($name, $value, $tableName = null)
    {
        $args = ['?', '?', '?'];
        $params = [$this->catalog, $name, $value];

        if (!is_null($tableName)) {
            array_push($args, 'table_name => ?');
            array_push($params, $tableName);
        }

        $this->execute('CALL ducklake_set_option(' . join(', ', $args) . ')', $params);
    }

    public function formatVersion()
    {
        return $this->execute('SELECT value FROM ducklake_options(?) WHERE option_name = ?', [$this->catalog, 'version'])->toArray()[0]['value'];
    }

    // experimental
    public function extensionVersion()
    {
        return $this->execute('SELECT extension_version FROM duckdb_extensions() WHERE extension_name = ?', ['ducklake'])->toArray()[0]['extension_version'];
    }

    // experimental
    public function duckdbVersion()
    {
        return $this->execute('SELECT VERSION() AS version')->toArray()[0]['version'];
    }

    // https://ducklake.select/docs/stable/duckdb/maintenance/merge_adjacent_files
    public function mergeAdjacentFiles()
    {
        $this->execute('CALL merge_adjacent_files()');
    }

    // https://ducklake.select/docs/stable/duckdb/maintenance/expire_snapshots
    public function expireSnapshots($olderThan = null, $dryRun = false)
    {
        $args = ['?'];
        $params = [$this->catalog];

        if (!is_null($olderThan)) {
            array_push($args, 'older_than => ?');
            array_push($params, $olderThan);
        }

        if ($dryRun) {
            array_push($args, 'dry_run => ?');
            array_push($params, $dryRun);
        }

        return $this->execute('CALL ducklake_expire_snapshots(' . join(', ', $args) . ')', $params)->toArray();
    }

    // https://ducklake.select/docs/stable/duckdb/maintenance/cleanup_old_files
    public function cleanupOldFiles($cleanupAll = false, $olderThan = null, $dryRun = false)
    {
        $args = ['?'];
        $params = [$this->catalog];

        if ($cleanupAll) {
            array_push($args, 'cleanup_all => ?');
            array_push($params, $cleanupAll);
        }

        if (!is_null($olderThan)) {
            array_push($args, 'older_than => ?');
            array_push($params, $olderThan);
        }

        if ($dryRun) {
            array_push($args, 'dry_run => ?');
            array_push($params, $dryRun);
        }

        return $this->execute('CALL ducklake_cleanup_old_files(' . join(', ', $args) . ')', $params)->toArray();
    }

    // https://ducklake.select/docs/stable/duckdb/maintenance/rewrite_data_files
    public function rewriteDataFiles($table = null, $deleteThreshold = null)
    {
        $args = ['?'];
        $params = [$this->catalog];

        if (!is_null($table)) {
            array_push($args, '?');
            array_push($params, $table);
        }

        if (!is_null($deleteThreshold)) {
            array_push($args, 'delete_threshold => ?');
            array_push($params, $deleteThreshold);
        }

        $this->execute('CALL ducklake_rewrite_data_files(' . join(', ', $args) . ')', $params);
    }

    // experimental
    // https://ducklake.select/docs/stable/duckdb/maintenance/checkpoint
    public function checkpoint()
    {
        $this->execute('CHECKPOINT');
    }

    // https://ducklake.select/docs/stable/duckdb/advanced_features/data_inlining
    public function flushInlinedData($tableName = null)
    {
        $args = ['?'];
        $params = [$this->catalog];

        if (!is_null($tableName)) {
            array_push($args, 'table_name => ?');
            array_push($params, $tableName);
        }

        $this->execute('CALL ducklake_flush_inlined_data(' . join(', ', $args) . ')', $params);
    }

    // https://ducklake.select/docs/stable/duckdb/metadata/list_files
    public function listFiles($table, $snapshotVersion = null, $snapshotTime = null)
    {
        $args = ['?', '?'];
        $params = [$this->catalog, $table];

        if (!is_null($snapshotVersion)) {
            array_push($args, 'snapshot_version => ?');
            array_push($params, $snapshotVersion);
        }

        if (!is_null($snapshotTime)) {
            array_push($args, 'snapshot_time => ?');
            array_push($params, $snapshotTime);
        }

        return $this->execute('SELECT * FROM ducklake_list_files(' . join(', ', $args) . ')', $params)->toArray();
    }

    // https://ducklake.select/docs/stable/duckdb/metadata/adding_files
    public function addDataFiles($table, $data, $allowMissing = null, $ignoreExtraColumns = null)
    {
        $params = [$this->catalog, $table, $data];
        $args = ['?', '?', '?'];

        if (!is_null($allowMissing)) {
            array_push($args, 'allow_missing => ?');
            array_push($params, $allowMissing);
        }

        if (!is_null($ignoreExtraColumns)) {
            array_push($args, 'ignore_extra_columns => ?');
            array_push($params, $ignoreExtraColumns);
        }

        $this->execute('CALL ducklake_add_data_files(' . join(', ', $args) . ')', $params);
    }

    // libduckdb does not provide function
    // https://duckdb.org/docs/stable/sql/dialect/keywords_and_identifiers.html
    public function quoteIdentifier($value)
    {
        return '"' . str_replace('"', '""', $value) . '"';
    }

    // libduckdb does not provide function
    // TODO support more types
    public function quote($value)
    {
        if (is_null($value)) {
            return 'NULL';
        } elseif ($value === true) {
            return 'true';
        } elseif ($value === false) {
            return 'false';
        } elseif (is_int($value) || is_float($value)) {
            return (string) $value;
        } else {
            if ($value instanceof \DateTime) {
                $value = $value->format('Y-m-d\TH:i:s.up');
            }

            if (is_string($value)) {
                return "'" . str_replace("'", "''", $value) . "'";
            } else {
                throw new \TypeError("can't quote");
            }
        }
    }

    private function execute($sql, $params = [])
    {
        $stmt = $this->db->preparedStatement($sql);
        for ($i = 0; $i < count($params); $i++) {
            $param = $params[$i];
            if ($param instanceof \DateTime) {
                $param = \Saturio\DuckDB\Type\Timestamp::fromDatetime($param);
            }
            $stmt->bindParam($i + 1, $param);
        }
        $result = $stmt->execute();
        return new Result([...$result->columnNames()], [...$result->rows()]);
    }

    private function installExtension($extension)
    {
        $this->execute('INSTALL ' . $this->quoteIdentifier($extension));
    }

    private function createSecret($options)
    {
        $this->execute('CREATE SECRET (' . $this->optionsArgs($options) . ')');
    }

    private function attachWithOptions($alias, $url, $options)
    {
        $this->execute('ATTACH ' . $this->quote($url) . ' AS ' . $this->quoteIdentifier($alias) . ' (' . $this->optionsArgs($options) . ')');
    }

    private function optionsArgs($options)
    {
        return join(', ', array_map(fn ($k, $v) => $this->optionName($k) . ' ' . $this->quote($v), array_keys($options), $options));
    }

    private function optionName($k)
    {
        $name = strtoupper($k);
        // should never contain user input, but just to be safe
        if (!preg_match('/^[A-Z_]+$/', $name)) {
            throw new \InvalidArgumentException('Invalid option name');
        }
        return $name;
    }
}
