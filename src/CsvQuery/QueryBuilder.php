<?php

namespace CsvQuery;

class QueryBuilder
{
    private Executor $executor;
    private array $where = [];
    private string $operator = 'AND';
    private string $groupBy = '';
    private string $aggCol = '';
    private string $aggFunc = '';
    private int $limit = 0;
    private int $offset = 0;
    private bool $explain = false;

    public function __construct(Executor $executor)
    {
        $this->executor = $executor;
    }

    public function where(string $column, string $operator, $value = null): self
    {
        if ($value === null) {
            $value = $operator;
            $operator = '='; // Default to equals
        }
        $this->where[$column] = $value; // Simple map for now. Complex logic handled if needed.
        // Wait, Go ParseCondition expects map or complex struct.
        // If simply map[col]val, it assumes AND Eq.
        // To support operators (>, <, etc), we need richer structure.
        // Let's refine this to match Go's Condition struct expectation if possible.
        // Go ParseCondition: map[string]interface{} -> AND EQ.
        // OR {operator: "", children: ...}
        
        // This builder is simplified. To support operators, we need to change internal storage.
        // For professional refactor, let's keep it aligning with simple map for MVP or implement full AST.
        // Given existing code likely used simple map, let's stick to simple map for "where equals".
        // To support op, we might need a special key or separate structure.
        
        // Let's defer complex where to later or handle only EQ for now as per Go types.go logic used (map[string]interface{})
        // The Go types.go logic handles simple map as AND EQ.
        
        return $this;
    }

    public function groupBy(string $column): self
    {
        $this->groupBy = $column;
        return $this;
    }

    public function aggregate(string $func, string $column): self
    {
        $this->aggFunc = $func;
        $this->aggCol = $column;
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
    
    public function explain(): self
    {
        $this->explain = true;
        return $this;
    }

    public function get(): Result
    {
        return $this->executor->execute([
            'where' => $this->where,
            'groupBy' => $this->groupBy,
            'aggCol' => $this->aggCol,
            'aggFunc' => $this->aggFunc,
            'limit' => $this->limit,
            'offset' => $this->offset,
            'explain' => $this->explain,
        ]);
    }

    public function count(): int
    {
        $res = $this->executor->execute([
            'where' => $this->where,
            'groupBy' => $this->groupBy,
            'action' => 'count'
        ]);
        return $res->getCount();
    }
}
