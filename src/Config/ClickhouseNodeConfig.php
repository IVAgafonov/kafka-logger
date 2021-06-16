<?php

namespace ClickhouseClusterClient\Config;

class ClickhouseNodeConfig
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
     * @var string
     */
    public $database = 'default';

    /**
     * AbstractExternalServiceConfig constructor.
     * @param string $host
     * @param int $port
     * @param string $database
     * @param string|null $login
     * @param string|null $password
     */
    public function __construct(string $host, int $port, string $database = 'default', ?string $login = 'default', ?string $password = null)
    {
        $this->host = $host;
        $this->port = $port;
        $this->login = $login;
        $this->password = $password;
        $this->database = $database;
    }
}
