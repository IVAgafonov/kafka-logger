<?php

namespace KafkaLogger\Logger;

/**
 *
 */
class GelfKafkaLogger extends KafkaLogger
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
                array_merge(
                    [
                        'date' => substr($date, 0, 10),
                        'pid' => $this->pid,
                        'server' => $this->server,
                        'env' => $this->env,
                        'app' => $this->app,
                        'service' => $this->service,
                        'user_id' => $this->user_id,
                        'project_type' => $this->project_type,
                        'project_id' => $this->project_id,
                        'item_type' => $this->item_type,
                        'item_id' => $this->item_id,
                        'level' => $level,
                        'message' => $message,
                        'source' => $this->app.'_'.$this->env.'_'.$this->server,
                    ],
                    $context
                )
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
