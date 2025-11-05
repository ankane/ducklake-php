<?php

namespace DuckLake;

class Result
{
    private $columns;
    private $rows;

    public function __construct($columns, $rows)
    {
        $this->columns = $columns;
        $this->rows = $rows;
    }

    public function columns()
    {
        return $this->columns;
    }

    public function rows()
    {
        return $this->rows;
    }

    public function toArray()
    {
        return array_map(fn ($row) => array_combine($this->columns, $row), $this->rows);
    }
}
