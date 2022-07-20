<?php

namespace KafkaLogger\Logger;

use KafkaLogger\Config\KafkaConfig;
use Psr\Log\LogLevel;
use RdKafka\Producer;

/**
 *
 */
class ClickhouseKafkaLogger extends KafkaLogger
{

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
                    'Ord' => ++$this->ord,
                    'Pid' => $this->pid,
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
                    'ExtraData' => (string) json_encode($context)
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
