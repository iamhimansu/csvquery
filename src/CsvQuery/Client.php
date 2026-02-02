<?php

namespace CsvQuery;

class Client
{
    private Config $config;

    public function __construct(Config $config)
    {
        $this->config = $config;
    }

    public function query(array $payload): array
    {
        $payload['action'] = $payload['action'] ?? 'query';
        $payload['csv'] = $this->config->getCsvPath();
        $payload['indexDir'] = $this->config->getIndexDir();

        return $this->execute($payload);
    }

    public function index(array $columns, array $options = []): array
    {
        $payload = [
            'action' => 'index',
            'csv' => $this->config->getCsvPath(),
            'cols' => json_encode($columns),
            'out' => $this->config->getIndexDir(),
            'sep' => $options['separator'] ?? ',',
            'workers' => $options['workers'] ?? 4,
            'memory' => $options['memory'] ?? 512,
            'bloom_rate' => $options['bloom_rate'] ?? 0.01,
            'verbose' => $options['verbose'] ?? false,
        ];
        return $this->execute($payload);
    }

    private function execute(array $payload): array
    {
        $bin = $this->config->getBinPath();
        if (!file_exists($bin)) {
            throw new \RuntimeException("Binary not found at: $bin");
        }

        $descriptorSpec = [
            0 => ["pipe", "r"], // stdin
            1 => ["pipe", "w"], // stdout
            2 => ["pipe", "w"], // stderr
        ];

        $cmd = escapeshellcmd($bin);
        // Pass request via stdin
        $process = proc_open($cmd, $descriptorSpec, $pipes);

        if (!is_resource($process)) {
            throw new \RuntimeException("Failed to start process");
        }

        fwrite($pipes[0], json_encode($payload));
        fclose($pipes[0]);

        $stdout = stream_get_contents($pipes[1]);
        fclose($pipes[1]);

        $stderr = stream_get_contents($pipes[2]);
        fclose($pipes[2]);
        
        if ($stderr) {
            fwrite(STDERR, $stderr);
        }

        $exitCode = proc_close($process);

        if ($exitCode !== 0) {
            throw new \RuntimeException("Go binary failed (Exit $exitCode): $stderr");
        }
        
        // Debug
        // echo "STDOUT: " . substr($stdout, 0, 100) . "\n";

        // Protocol: If JSON, return decoded. If simple count, wrap.
        // The binary mostly outputs raw lines for query, but JSON for 'explain' or 'status'.
        // If action is 'query' and not explain/count, we get "offset,line\n".
        // If action is count, we get number.
        // If action is index, we get JSON.
        
        $action = $payload['action'] ?? 'query';
        
        if ($action === 'index') {
            return json_decode($stdout, true) ?? ['error' => 'Invalid JSON output'];
        }
        
        if (!empty($payload['explain'])) {
             // Expect JSON plan usually, roughly
             // Executor output: "Plan: map[...]" which is NOT JSON. Use Go's JSON output for explain?
             // Main.go handled explain via executor. 
             // Executor writes "Plan: ..."
             // This needs adjustment in Go if we want valid JSON.
             // For now return raw string wrapped
             return ['output' => $stdout]; 
        }

        if ($action === 'count' || !empty($payload['countOnly'])) {
            return ['count' => (int)trim($stdout)];
        }
        
        // Aggregation returns JSON
        if (!empty($payload['groupBy'])) {
            $data = json_decode($stdout, true);
            if (json_last_error() === JSON_ERROR_NONE) {
                return ['status' => 'ok', 'groups' => $data];
            } else {
                // Fallback or error?
                return ['status' => 'error', 'error' => 'Invalid aggregation output: ' . $stdout];
            }
        }
        
        // Standard query lines
        $lines = array_filter(explode("\n", trim($stdout)));
        $rows = [];
        foreach ($lines as $line) {
            $parts = explode(",", $line);
            if (count($parts) >= 2) {
                $rows[] = [
                    'offset' => (int)$parts[0],
                    'line' => (int)$parts[1]
                ];
            }
        }
        return ['status' => 'ok', 'rows' => $rows];
    }
}
