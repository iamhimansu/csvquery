<?php

namespace CsvQuery;

class Config
{
    private string $binPath;
    private string $csvPath;
    private string $indexDir;
    private int $timeout;
    private int $memoryLimit;

    public function __construct(string $csvPath, string $binPath = null)
    {
        $this->csvPath = $csvPath;
        $this->binPath = $binPath ?? dirname(__DIR__, 2) . '/bin/csvquery';
        $this->indexDir = dirname($csvPath);
        $this->timeout = 60;
        $this->memoryLimit = 1024; // MB
    }

    public function setBinPath(string $path): self
    {
        $this->binPath = $path;
        return $this;
    }

    public function setIndexDir(string $path): self
    {
        $this->indexDir = $path;
        return $this;
    }

    public function setTimeout(int $seconds): self
    {
        $this->timeout = $seconds;
        return $this;
    }

    public function getBinPath(): string
    {
        return $this->binPath;
    }

    public function getCsvPath(): string
    {
        return $this->csvPath;
    }

    public function getIndexDir(): string
    {
        return $this->indexDir;
    }

    public function getTimeout(): int
    {
        return $this->timeout;
    }
}
