<?php

namespace ClickhouseClusterClient\Client;

use ClickHouseDB\Statement;

/**
 * Interface ClickhouseClusterClientInterface
 * @package ClickhouseClusterClient\Client
 */
interface ClickhouseClusterClientInterface
{
    /**
     * Use basename
     *
     * @param string $database Base name
     */
    public function database(string $database = 'default'): void;

    /**
     * Execute query
     *
     * @param string $sql Query
     *
     * @return Statement
     */
    public function query(string $sql): Statement;

    /**
     * Execute query and get result as arrays
     *
     * @param string $sql Query
     *
     * @return mixed
     */
    public function getArrays(string $sql);

    /**
     * Execute query and get result as array
     *
     * @param string $sql Query
     *
     * @return mixed
     */
    public function getArray(string $sql);

    /**
     * Execute query and get result as value
     *
     * @param string $sql Query
     *
     * @return mixed
     */
    public function getValue(string $sql);

    /**
     * Quote string
     *
     * @param mixed $string Str
     *
     * @return string
     */
    public function quote($string): string;

    /**
     * Insert assoc array batch
     *
     * @param string $table
     * @param array $rows
     *
     * @return void
     */
    public function insertMultiple(string $table, array $rows): void;

    /**
     * Truncate table
     *
     * @param string $table
     *
     * @return void
     */
    public function truncateTable(string $table): void;
}