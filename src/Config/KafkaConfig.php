<?php

namespace KafkaLogger\Config;

class KafkaConfig
{
    /**
     * @var string[]
     */
    public $hosts;

    /**
     * @var string
     */
    public $topic;

    /**
     * AbstractExternalServiceConfig constructor.
     * @param string[] $hosts
     * @param string $topic
     */
    public function __construct(array $hosts, string $topic)
    {
        $this->hosts = $hosts;
        $this->topic = $topic;
    }
}
