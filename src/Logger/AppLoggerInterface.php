<?php

namespace KafkaLogger\Logger;

use Psr\Log\LoggerInterface;

interface AppLoggerInterface extends LoggerInterface
{
    /**
     * @param string $server
     * @return $this
     */
    public function setServer(string $server): self;
    /**
     * @param string $env
     * @return $this
     */
    public function setEnv(string $env): self;

    /**
     * @param string $app
     * @return $this
     */
    public function setApp(string $app): self;

    /**
     * @param int $user_id
     * @return $this
     */
    public function setUserId(int $user_id): self;

    /**
     * @param string $service
     * @return $this
     */
    public function setService(string $service): self;

    /**
     * @param string $project_type
     * @return $this
     */
    public function setProjectType(string $project_type): self;

    /**
     * @param int $project_id
     * @return $this
     */
    public function setProjectId(int $project_id): self;

    /**
     * @param string $item_type
     * @return $this
     */
    public function setItemType(string $item_type): self;

    /**
     * @param int $item_id
     * @return $this
     */
    public function setItemId(int $item_id): self;
}
