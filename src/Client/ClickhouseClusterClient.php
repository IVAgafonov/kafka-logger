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
     * ClickhouseClusterClient constructor.
     * @param ClickhouseNodeConfig[] $clickhouseNodeConfigs
     * @param RedisConfig $redisConfig
     */
    public function __construct(array $clickhouseNodeConfigs, RedisConfig $redisConfig)
    {
        $this->redisInstance = new Redis();
        $this->redisInstance->connect($redisConfig->host, $redisConfig->port);
        if ($redisConfig->password) {
            $this->redisInstance->auth($redisConfig->password);
        }
        $this->redisInstance->setOption(\Redis::OPT_SERIALIZER, (string) \Redis::SERIALIZER_PHP);
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
                sleep(1000);
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
            $instance->database($database);
        }
    }

    /**
     * @inheritdoc
     */
    public function query(string $sql): Statement
    {
        //to cache

        shuffle($this->clickhouseInstances);

        foreach ($this->clickhouseInstances as $instance) {
            try {
                return $instance->write($sql);
            } catch (\Throwable $e) {
                sleep(1000);
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
                sleep(1000);
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
                sleep(1000);
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
                sleep(1000);
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
        shuffle($this->clickhouseInstances);

        foreach ($this->clickhouseInstances as $instance) {
            try {
                $rows = array_values($rows);
                $instance->insertAssocBulk($table, $rows);
                return;
            } catch (\Throwable $e) {
                sleep(1000);
            }
        }
        throw new \RuntimeException('Clickhouse cluster is unavailable');
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
                sleep(1000);
            }
        }
        throw new \RuntimeException('Clickhouse cluster is unavailable');
    }
}
