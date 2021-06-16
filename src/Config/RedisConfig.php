<?php

namespace ClickhouseClusterClient\Config;

class RedisConfig
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
