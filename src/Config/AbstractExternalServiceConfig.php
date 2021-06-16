<?php

namespace ClickhouseClusterClient\Config;

/**
 * Class AbstractExternalServiceConfig
 * @package ClickhouseClusterClient\Config
 */
abstract class AbstractExternalServiceConfig
{
    /**
     * @var string
     */
    public $host = '';

    /**
     * @var int
     */
    public $port = 0;

    /**
     * @var string|null
     */
    public $login = null;

    /**
     * @var string|null
     */
    public $password = null;

    /**
     * AbstractExternalServiceConfig constructor.
     * @param string $host
     * @param int $port
     * @param string|null $password
     */
    public function __construct(string $host, int $port, ?string $password = null)
    {
        $this->host = $host;
        $this->port = $port;
        $this->password = $password;
    }
}
