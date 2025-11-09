<?php

namespace DuckLake;

class Library
{
    public static function check($event = null)
    {
        return \Saturio\DuckDB\CLib\Installer::install($event);
    }
}
