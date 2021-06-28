<?php

namespace ClickhouseClusterClient\Client;

use ClickhouseClusterClient\Config\ClickhouseNodeConfig;
use ClickhouseClusterClient\Config\RedisConfig;
use ClickHouseDB\Client;
use ClickHouseDB\Statement;
use Redis;

/**
 * Class ClickhouseClusterClient
 * @package ClickhouseClusterClient\Client
 */
class ClickhouseClusterClient implements ClickhouseClusterClientInterface
{
    /**
     * @var Redis
     */
    private $redisInstance;

    /**
     * @var Client[]
     */
    private $clickhouseInstances;

    /**
     * @var int
     */
    private $listLimit = 20000;

    /**
     * @var int
     */
    private $timeLimit = 60;

    /**
     * @var string
     */
    private $redisKey = 'ch_cluster_cache:';

    /**
     * ClickhouseClusterClient constructor.
     * @param ClickhouseNodeConfig[] $clickhouseNodeConfigs
     * @param RedisConfig|null $redisConfig
     * @param Redis|null $redisInstance
     */
    public function __construct(array $clickhouseNodeConfigs, RedisConfig $redisConfig = null, Redis $redisInstance = null)
    {
        if (!$redisInstance) {
            $this->redisInstance = new Redis();
            $this->redisInstance->connect($redisConfig->host, $redisConfig->port);
            if ($redisConfig->password) {
                $this->redisInstance->auth($redisConfig->password);
            }
            $this->redisInstance->setOption(\Redis::OPT_SERIALIZER, (string) \Redis::SERIALIZER_PHP);
        } else {
            $this->redisInstance = $redisInstance;
        }
        if (!$this->redisInstance) {
            throw new \RuntimeException('Invalid redis config');
        }
        foreach ($clickhouseNodeConfigs as $clickhouseNodeConfig) {
            try {
                $client = new Client([
                    'host' => $clickhouseNodeConfig->host,
                    'port' => $clickhouseNodeConfig->port,
                    'username' => $clickhouseNodeConfig->login,
                    'password' => $clickhouseNodeConfig->password,
                    'database' => $clickhouseNodeConfig->database
                ]);
                $client->database($clickhouseNodeConfig->database);
                $client->setTimeout(1.5);
                $client->setTimeout(10);
                $client->setConnectTimeOut(5);
                $this->clickhouseInstances[] = $client;
            } catch (\Throwable $e) {
                file_put_contents('/tmp/clickhouse_cluster_debug', "host: ".$clickhouseNodeConfig->host."; message: ".$e->getMessage()."\n", FILE_APPEND);
                sleep(1);
            }
        }
        if (!count($this->clickhouseInstances)) {
            throw new \RuntimeException('Clickhouse cluster is unavailable');
        }
    }

    public function getClient(): Client
    {
        return $this->clickhouseInstances[array_rand($this->clickhouseInstances)];
    }

    /**
     * @inheritdoc
     */
    public function database(string $database = 'default'): void
    {
        foreach ($this->clickhouseInstances as $instance) {
            try {
                $instance->database($database);
            } catch (\Throwable $e) {
                $instance->database($database);
                sleep(1);
            }
        }
    }

    /**
     * @inheritdoc
     */
    public function query(string $sql): Statement
    {
        shuffle($this->clickhouseInstances);

        foreach ($this->clickhouseInstances as $instance) {
            try {
                return $instance->write($sql);
            } catch (\Throwable $e) {
                file_put_contents('/tmp/clickhouse_cluster_debug', "host: ".$instance->getConnectHost()."; message: ".$e->getMessage()."\n", FILE_APPEND);
                sleep(1);
            }
        }
        throw new \RuntimeException('Clickhouse cluster is unavailable');
    }

    /**
     * @deprecated
     *
     * Execute query
     *
     * @param string $sql Query
     *
     * @return Statement
     */
    public function queryF(string $sql): Statement
    {
        return $this->query($sql);
    }

    /**
     * @inheritdoc
     */
    public function getArrays(string $sql)
    {
        shuffle($this->clickhouseInstances);

        foreach ($this->clickhouseInstances as $instance) {
            try {
                $res = $instance->select($sql);
                $array = $res->rows();
                return $array ?: [];
            } catch (\Throwable $e) {
                file_put_contents('/tmp/clickhouse_cluster_debug', "host: ".$instance->getConnectHost()."; message: ".$e->getMessage()."\n", FILE_APPEND);
                sleep(1);
            }
        }
        throw new \RuntimeException('Clickhouse cluster is unavailable');
    }

    /**
     * @inheritdoc
     */
    public function getArray(string $sql)
    {
        shuffle($this->clickhouseInstances);

        foreach ($this->clickhouseInstances as $instance) {
            try {
                $res = $instance->select($sql);
                $array = $res->rows();
                $retArray = current($array);
                return $retArray ?: [];
            } catch (\Throwable $e) {
                file_put_contents('/tmp/clickhouse_cluster_debug', "host: ".$instance->getConnectHost()."; message: ".$e->getMessage()."\n", FILE_APPEND);
                sleep(1);
            }
        }
        throw new \RuntimeException('Clickhouse cluster is unavailable');
    }

    /**
     * Execute query and get result as value
     *
     * @param string $sql Query
     *
     * @return string|null
     */
    public function getValue(string $sql): ?string
    {
        shuffle($this->clickhouseInstances);

        foreach ($this->clickhouseInstances as $instance) {
            try {
                $res = $instance->select($sql);
                $array = $res->rows();
                if (!is_array($array) || !$array) {
                    return null;
                }
                $result = $array;
                while (is_array($result)) {
                    $result = current($result);
                    if (!is_array($result)) {
                        return $result;
                    }
                }
                return null;
            } catch (\Throwable $e) {
                file_put_contents('/tmp/clickhouse_cluster_debug', "host: ".$instance->getConnectHost()."; message: ".$e->getMessage()."\n", FILE_APPEND);
                sleep(1);
            }
        }
        throw new \RuntimeException('Clickhouse cluster is unavailable');
    }

    /**
     * @deprecated
     *
     * @param mixed $string
     *
     * @return string
     */
    public function quoteString($string): string
    {
        return $this->quote($string);
    }

    /**
     * @inheritdoc
     */
    public function quote($string): string
    {
        if (is_bool($string)) {
            return $string ? 1 : 0;
        } elseif (is_numeric($string) && preg_match('#^-?(?:\d+\.)?\d+$#', $string)) {
            return $string;
        }
        $string = addslashes($string);
        return "'" . str_replace('\\"', '"', $string) . "'";
    }

    /**
     * @inheritdoc
     */
    public function insertMultiple(string $table, array $rows): void
    {
        $this->redisInstance->sAdd($this->redisKey."table_list", $table);
        $this->setIntoCache($table, $rows);
        $this->writeCache($table);
    }

    /**
     * @inheritdoc
     */
    public function truncateTable(string $table): void
    {
        shuffle($this->clickhouseInstances);

        foreach ($this->clickhouseInstances as $instance) {
            try {
                $instance->truncateTable($table);
                return;
            } catch (\Throwable $e) {
                file_put_contents('/tmp/clickhouse_cluster_debug', "host: ".$instance->getConnectHost()."; message: ".$e->getMessage()."\n", FILE_APPEND);
                sleep(1);
            }
        }
        throw new \RuntimeException('Clickhouse cluster is unavailable');
    }

    private function setIntoCache(string $table, array $rows): void
    {
        file_put_contents('/tmp/clickhouse_cluster_debug', "write block into cache\n", FILE_APPEND);
        $this->redisInstance->setnx($this->redisKey.$table.':ts', time());
        $this->redisInstance->rPush($this->redisKey.$table, ...$rows);
//        if (!isset($this->inmemoryCache[$table])) {
//
//            $this->inmemoryCache[$table] = [
//                'timestamp' => time(),
//                'list' => array_values($rows)
//            ];
//        } else {
//            $this->inmemoryCache[$table]['list'] = array_merge($this->inmemoryCache[$table]['list'], array_values($rows));
//        }
    }

    private function getFromCache(string $table): array
    {
        $this->redisInstance->del($this->redisKey.$table.':ts');
        return $this->redisInstance->lRange($this->redisKey.$table, 0, -1);
    }

    /**
     * @param string $table
     * @param bool $force
     */
    private function writeCache(string $table, bool $force = false): void
    {
        if ($force ||
            ((int) $this->redisInstance->get($this->redisKey.$table.':ts')) + $this->timeLimit < time() ||
            $this->redisInstance->lLen($this->redisKey.$table) > $this->listLimit
        ) {
            file_put_contents('/tmp/clickhouse_cluster_debug', "write block into clickhouse\n", FILE_APPEND);
            shuffle($this->clickhouseInstances);

            $this->redisInstance->multi();
            $this->redisInstance->lRange($this->redisKey.$table, 0, -1);
            $this->redisInstance->del($this->redisKey.$table);
            $this->redisInstance->del($this->redisKey.$table.':ts');
            $rows = $this->redisInstance->exec();

            foreach ($this->clickhouseInstances as $instance) {
                try {
                    if (!empty($rows[0])) {
                        $rowsBulkByFields = [];
                        foreach ($rows[0] as $row) {
                            $keys = implode("", array_keys($row));
                            if (!isset($rowsBulkByFields[$keys])) {
                                $rowsBulkByFields[$keys] = [];
                            }
                            $rowsBulkByFields[$keys][] = $row;
                        }
                        foreach ($rowsBulkByFields as $rowsBulk) {
                            $instance->insertAssocBulk($table, $rowsBulk);
                        }
                        file_put_contents('/tmp/clickhouse_cluster_debug', "host: ".$instance->getConnectHost()."; message: successfully written\n", FILE_APPEND);
                    }
                    return;
                } catch (\Throwable $e) {
                    file_put_contents('/tmp/clickhouse_cluster_debug', "host: ".$instance->getConnectHost()."; message: ".$e->getMessage()."\n", FILE_APPEND);
                    sleep(1);
                }
            }
            throw new \RuntimeException('Clickhouse cluster is unavailable');
        }
    }

    /**
     * @param bool $force
     */
    public function dump($force = false)
    {
        $tables = $this->redisInstance->sMembers($this->redisKey."table_list");
        foreach ($tables as $table) {
            $this->writeCache($table, $force);
        }
    }
}
