<?php
/**
 * Executor - Handles execution of Go binary commands
 */

declare(strict_types=1);

namespace iamhimansu\csvquery;

use RuntimeException;
use Generator;

class Executor
{
    private string $binaryPath;
    private int $workers;
    private string $indexDir;
    private bool $useSocket;

    public function __construct(array $options = [])
    {
        $this->workers = $options['workers'] ?? 0;
        $this->indexDir = $options['indexDir'] ?? '';
        $this->useSocket = $options['useSocket'] ?? true;
        $this->binaryPath = $options['binaryPath'] ?? $this->detectBinary();

        if (!file_exists($this->binaryPath)) {
            throw new RuntimeException("CsvQuery binary not found: {$this->binaryPath}");
        }

        if ($this->useSocket) {
            SocketClient::configure($this->binaryPath, $this->indexDir);
        }
    }

    private function detectBinary(): string
    {
        $binDir = dirname(__DIR__) . '/bin';
        $os = match (PHP_OS_FAMILY) {
            'Darwin' => 'darwin',
            'Windows' => 'windows',
            default => 'linux',
        };
        $arch = match (php_uname('m')) {
            'arm64', 'aarch64' => 'arm64',
            default => 'amd64',
        };
        $ext = $os === 'windows' ? '.exe' : '';
        
        $binary = "{$binDir}/csvquery_{$os}_{$arch}{$ext}";
        if (!file_exists($binary)) {
             $binary = "{$binDir}/csvquery{$ext}";
        }
        return $binary;
    }

    public function createIndex(string $csvPath, string $outputDir, string $columnsJson, string $separator = ','): bool
    {
        $args = [
            'index',
            '--input', escapeshellarg($csvPath),
            '--output', escapeshellarg($outputDir),
            '--columns', escapeshellarg($columnsJson),
            '--separator', escapeshellarg($separator),
        ];

        if ($this->workers > 0) {
            $args[] = '--workers';
            $args[] = (string)$this->workers;
        }

        $cmd = escapeshellcmd($this->binaryPath) . ' ' . implode(' ', $args);
        exec($cmd . ' 2>&1', $output, $exitCode);

        if ($exitCode !== 0) {
            throw new RuntimeException("Index creation failed: " . implode("\n", $output));
        }

        return true;
    }

    public function query(
        string $csvPath,
        string $indexDir,
        array $where,
        int $limit = 0,
        int $offset = 0,
        ?string $groupBy = null,
        ?string $aggCol = null,
        ?string $aggFunc = null
    ): Generator|array {
        if ($this->useSocket) {
            try {
                return SocketClient::getInstance()->query('select', [
                    'csv' => $csvPath,
                    'where' => $where,
                    'limit' => $limit,
                    'offset' => $offset,
                    'groupBy' => $groupBy,
                    'aggCol' => $aggCol,
                    'aggFunc' => $aggFunc,
                ]);
            } catch (\Exception $e) {
                // Fallback to CLI or throw
            }
        }

        // CLI Fallback implementation...
        return $this->queryCli($csvPath, $indexDir, $where, $limit, $offset, $groupBy, $aggCol, $aggFunc);
    }

    private function queryCli($csvPath, $indexDir, $where, $limit, $offset, $groupBy, $aggCol, $aggFunc): Generator|array
    {
        $args = [
            'query',
            '--csv', $csvPath,
            '--index-dir', $indexDir,
            '--where', json_encode(empty($where) ? new \stdClass() : $where),
        ];

        if ($limit > 0) $args[] = "--limit $limit";
        if ($offset > 0) $args[] = "--offset $offset";
        if ($groupBy) $args[] = "--group-by $groupBy";
        if ($aggCol) $args[] = "--agg-col $aggCol";
        if ($aggFunc) $args[] = "--agg-func $aggFunc";

        $cmd = escapeshellcmd($this->binaryPath) . ' ' . implode(' ', $args);
        
        // If aggregation, expect JSON
        if ($groupBy || $aggFunc) {
            exec($cmd . ' 2>&1', $output, $exitCode);
            if ($exitCode !== 0) throw new RuntimeException("Query failed: " . implode("\n", $output));
            return json_decode(implode("\n", $output), true) ?: [];
        }

        // Otherwise stream offsets
        return $this->streamOffsets($cmd);
    }

    private function streamOffsets(string $cmd): Generator
    {
        $handle = popen($cmd . ' 2>&1', 'r');
        while (($line = fgets($handle)) !== false) {
            $line = trim($line);
            if ($line === '') continue;
            $parts = explode(',', $line);
            if (count($parts) >= 2) {
                yield [
                    'offset' => (int)$parts[0],
                    'line' => (int)$parts[1]
                ];
            }
        }
        pclose($handle);
    }

    public function count(string $csvPath, string $indexDir, array $where): int
    {
        if ($this->useSocket) {
             try {
                 return SocketClient::getInstance()->count($csvPath, $where);
             } catch (\Exception $e) {}
        }

        $args = [
            'query',
            '--csv', $csvPath,
            '--index-dir', $indexDir,
            '--where', json_encode(empty($where) ? new \stdClass() : $where),
            '--count'
        ];
        $cmd = escapeshellcmd($this->binaryPath) . ' ' . implode(' ', array_map('escapeshellarg', $args));
        exec($cmd . ' 2>&1', $output, $exitCode);
        return (int)trim(implode("\n", $output));
    }
}
