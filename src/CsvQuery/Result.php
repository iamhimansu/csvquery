<?php

namespace CsvQuery;

class Result implements \IteratorAggregate, \Countable
{
    private array $data;
    private array $rows;
    private $groups; // Can be array or null

    public function __construct(array $data)
    {
        $this->data = $data;
        $this->rows = $data['rows'] ?? [];
        $this->groups = $data['groups'] ?? null;
    }

    public function getStatus(): string
    {
        return $this->data['status'] ?? 'unknown';
    }

    public function getRows(): array
    {
        return $this->rows;
    }
    
    public function getGroups()
    {
        return $this->groups;
    }

    public function getCount(): int
    {
        return $this->data['count'] ?? count($this->rows);
    }
    
    public function count(): int
    {
        return $this->getCount();
    }

    public function getIterator(): \Traversable
    {
        return new \ArrayIterator($this->rows);
    }
    
    public function toArray(): array
    {
        return $this->data;
    }
}
