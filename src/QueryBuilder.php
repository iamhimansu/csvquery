<?php
/**
 * QueryBuilder - Fluent Query Interface for CsvQuery
 */

declare(strict_types=1);

namespace iamhimansu\csvquery;

use Generator;

class QueryBuilder
{
    private CsvQuery $csv;
    private ?array $select = null;
    private array $where = [];
    private array $orderBy = [];
    private ?int $limit = null;
    private int $offset = 0;
    private array $groupBy = [];

    public function __construct(CsvQuery $csv)
    {
        $this->csv = $csv;
    }

    public function select(array $columns): self
    {
        $this->select = $columns;
        return $this;
    }

    public function where(array|string $condition, $value = null): self
    {
        if (is_string($condition) && $value !== null) {
            $this->where = ['=', $condition, $value];
        } else {
            $this->where = $condition;
        }
        return $this;
    }

    public function andWhere(array $condition): self
    {
        if (empty($this->where)) {
            $this->where = $condition;
        } else {
            $this->where = ['and', $this->where, $condition];
        }
        return $this;
    }

    public function limit(int $limit): self
    {
        $this->limit = $limit;
        return $this;
    }

    public function offset(int $offset): self
    {
        $this->offset = $offset;
        return $this;
    }

    public function groupBy(array|string $columns): self
    {
        $this->groupBy = (array)$columns;
        return $this;
    }

    public function orderBy(array $columns): self
    {
        $this->orderBy = $columns;
        return $this;
    }

    /**
     * Get all results as arrays.
     */
    public function all(): array
    {
        $results = [];
        foreach ($this->each() as $row) {
            $results[] = $row;
        }
        return $results;
    }

    /**
     * Get first result.
     */
    public function one(): ?array
    {
        $oldLimit = $this->limit;
        $this->limit = 1;
        $row = null;
        foreach ($this->each() as $r) {
            $row = $r;
            break;
        }
        $this->limit = $oldLimit;
        return $row;
    }

    /**
     * Count matching rows.
     */
    public function count(): int
    {
        return $this->csv->getExecutor()->count(
            $this->csv->getCsvPath(),
            $this->csv->getIndexDir(),
            $this->where
        );
    }

    /**
     * Iterates over results.
     */
    public function each(): Generator
    {
        $results = $this->csv->getExecutor()->query(
            $this->csv->getCsvPath(),
            $this->csv->getIndexDir(),
            $this->where,
            $this->limit ?? 0,
            $this->offset,
            !empty($this->groupBy) ? implode(',', $this->groupBy) : null
        );

        foreach ($results as $result) {
            if (isset($result['offset'])) {
                // Hydrate from offset
                $row = $this->csv->readRowAt((int)$result['offset']);
                if ($row) {
                    if ($this->select) {
                        $row = array_intersect_key($row, array_flip($this->select));
                    }
                    yield $row;
                }
            } else {
                // Return direct result (e.g. from aggregations)
                yield $result;
            }
        }
    }

    /**
     * Sum a column.
     */
    public function sum(string $column): float
    {
        $results = $this->csv->getExecutor()->query(
            $this->csv->getCsvPath(),
            $this->csv->getIndexDir(),
            $this->where,
            0,
            0,
            !empty($this->groupBy) ? implode(',', $this->groupBy) : $column,
            $column,
            'sum'
        );
        return (float)(reset($results) ?: 0);
    }
}
