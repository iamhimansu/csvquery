<?php

namespace CsvQuery;

class Executor
{
    private Client $client;

    public function __construct(Client $client)
    {
        $this->client = $client;
    }

    public function query(): QueryBuilder
    {
        return new QueryBuilder($this);
    }

    public function execute(array $params): Result
    {
        return new Result($this->client->query($params));
    }

    public function index(array $columns, array $options = []): array
    {
        return $this->client->index($columns, $options);
    }
}
