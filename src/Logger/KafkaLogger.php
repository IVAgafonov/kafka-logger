<?php

namespace KafkaLogger\Logger;

use KafkaLogger\Config\KafkaConfig;
use Psr\Log\LoggerInterface;
use Psr\Log\LogLevel;
use RdKafka\Producer;

/**
 *
 */
class KafkaLogger implements LoggerInterface {

    /**
     * @var string
     */
    private $app;

    /**
     * @var string
     */
    private $service;

    /**
     * @var \RdKafka\ProducerTopic
     */
    private $kafka_stream_topic;

    /**
     * @var Producer
     */
    private $producer;

    public function __construct(KafkaConfig $config, string $app = 'default_app', string $service = 'default_service', string $log_level = LOG_INFO)
    {
        $this->app = $app;
        $this->service = $service;

        $conf = new \RdKafka\Conf();
        $conf->set('log_level', (string) $log_level);
        $conf->set('debug', 'all');

        $this->producer = new Producer($conf);
        $this->producer->addBrokers(implode(",", $config->hosts));

        $this->kafka_stream_topic = $this->producer->newTopic($config->topic);
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
     * @param string $service
     * @return $this
     */
    public function setService(string $service): self
    {
        $this->service = $service;
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
                    'App' => $this->app,
                    'Name' => $this->service,
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
