<?php

namespace KafkaLogger\Logger;

use KafkaLogger\Config\KafkaConfig;
use Psr\Log\LogLevel;
use RdKafka\Producer;

/**
 *
 */
class KafkaLogger implements AppLoggerInterface {

    /**
     * @var string
     */
    private $server = '';

    /**
     * @var string
     */
    private $env = '';

    /**
     * @var string
     */
    private $app = '';

    /**
     * @var int
     */
    private $user_id = 0;

    /**
     * @var string
     */
    private $service = '';

    /**
     * @var string
     */
    private $project_type = '';

    /**
     * @var int
     */
    private $project_id = 0;

    /**
     * @var string
     */
    private $item_type = '';

    /**
     * @var int
     */
    private $item_id = 0;

    /**
     * @var \RdKafka\ProducerTopic
     */
    private $kafka_stream_topic;

    /**
     * @var Producer
     */
    private $producer;

    public function __construct(KafkaConfig $config, string $app = 'default_app', string $service = 'default_service', int $log_level = LOG_INFO)
    {
        $this->app = $app;
        $this->service = $service;

        $conf = new \RdKafka\Conf();
        $conf->set('log_level', (string) $log_level);
        if ($log_level === LOG_DEBUG) {
            $conf->set('debug', 'all');
        }

        $this->producer = new Producer($conf);
        $this->producer->addBrokers(implode(",", $config->hosts));

        $this->kafka_stream_topic = $this->producer->newTopic($config->topic);
    }

    /**
     * @param string $server
     * @return $this
     */
    public function setServer(string $server): self
    {
        $this->server = $server;
        return $this;
    }

    /**
     * @param string $env
     * @return $this
     */
    public function setEnv(string $env): self
    {
        $this->env = $env;
        return $this;
    }

    /**
     * @param string $app
     * @return $this
     */
    public function setApp(string $app): self
    {
        $this->app = $app;
        return $this;
    }

    /**
     * @param int $user_id
     * @return $this
     */
    public function setUserId(int $user_id): self
    {
        $this->user_id = $user_id;
        return $this;
    }

    /**
     * @param string $service
     * @return $this
     */
    public function setService(string $service): self
    {
        $this->service = $service;
        return $this;
    }

    /**
     * @param string $project_type
     * @return $this
     */
    public function setProjectType(string $project_type): self
    {
        $this->project_type = $project_type;
        return $this;
    }

    /**
     * @param int $project_id
     * @return $this
     */
    public function setProjectId(int $project_id): self
    {
        $this->project_id = $project_id;
        return $this;
    }

    /**
     * @param string $item_type
     * @return $this
     */
    public function setItemType(string $item_type): self
    {
        $this->item_type = $item_type;
        return $this;
    }

    /**
     * @param int $item_id
     * @return $this
     */
    public function setItemId(int $item_id): self
    {
        $this->item_id = $item_id;
        return $this;
    }

    /**
     * @param string $message
     * @param array $context
     */
    public function emergency($message, array $context = array())
    {
        $this->log(LogLevel::EMERGENCY, $message, $context);
    }

    /**
     * @param string $message
     * @param array $context
     */
    public function alert($message, array $context = array())
    {
        $this->log(LogLevel::ALERT, $message, $context);
    }

    /**
     * @param string $message
     * @param array $context
     */
    public function critical($message, array $context = array())
    {
        $this->log(LogLevel::CRITICAL, $message, $context);
    }

    /**
     * @param string $message
     * @param array $context
     */
    public function error($message, array $context = array())
    {
        $this->log(LogLevel::ERROR, $message, $context);
    }

    /**
     * @param string $message
     * @param array $context
     */
    public function warning($message, array $context = array())
    {
        $this->log(LogLevel::WARNING, $message, $context);
    }

    /**
     * @param string $message
     * @param array $context
     */
    public function notice($message, array $context = array())
    {
        $this->log(LogLevel::NOTICE, $message, $context);
    }

    /**
     * @param string $message
     * @param array $context
     */
    public function info($message, array $context = array())
    {
        $this->log(LogLevel::INFO, $message, $context);
    }

    /**
     * @param string $message
     * @param array $context
     */
    public function debug($message, array $context = array())
    {
        $this->log(LogLevel::DEBUG, $message, $context);
    }

    /**
     * @param mixed $level
     * @param string $message
     * @param array $context
     */
    public function log($level, $message, array $context = array())
    {
        $date = date('Y-m-d H:i:s');
        $this->kafka_stream_topic->produce(
            rand(0, 2),
            0,
            json_encode(
                [
                    'Date' => substr($date, 0, 10),
                    'DateTime' => $date,
                    'Server' => $this->server,
                    'Env' => $this->env,
                    'App' => $this->app,
                    'Service' => $this->service,
                    'UserId' => $this->user_id,
                    'ProjectType' => $this->project_type,
                    'ProjectId' => $this->project_id,
                    'ItemType' => $this->item_type,
                    'ItemId' => $this->item_id,
                    'Level' => $level,
                    'Message' => $message,
                    'ExtraData' => json_encode($context)
                ]
            )
        );
        $start = microtime(true);
        while ($this->producer->getOutQLen() > 0) {
            $this->producer->poll(0);

            if (microtime(true) - $start > 10) {
                throw new \RuntimeException("Can't write log");
            }
        }
    }
}
