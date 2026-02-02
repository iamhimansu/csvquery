<?php
/**
 * SocketClient - Fast communication with Go daemon
 */

declare(strict_types=1);

namespace iamhimansu\csvquery;

use RuntimeException;
use Exception;

class SocketClient
{
    private static ?SocketClient $instance = null;
    private $socket = null;
    private string $socketPath;
    private string $binaryPath;
    private string $indexDir;

    private function __construct(string $binaryPath, string $indexDir = '', string $socketPath = '/tmp/csvquery.sock')
    {
        $this->binaryPath = $binaryPath;
        $this->indexDir = $indexDir;
        $this->socketPath = $socketPath;
    }

    public static function configure(string $binaryPath, string $indexDir = '', string $socketPath = '/tmp/csvquery.sock'): void
    {
        self::$instance = new self($binaryPath, $indexDir, $socketPath);
    }

    public static function getInstance(): self
    {
        if (self::$instance === null) {
            throw new RuntimeException("SocketClient not configured");
        }
        return self::$instance;
    }

    public function query(string $action, array $params = []): array
    {
        $this->ensureConnected();
        $request = array_merge(['action' => $action], $params);
        fwrite($this->socket, json_encode($request) . "\n");
        $response = fgets($this->socket);
        
        if ($response === false) {
             throw new RuntimeException("No response from daemon");
        }

        $data = json_decode($response, true);
        if ($data === null) throw new RuntimeException("Invalid JSON: $response");
        if (!empty($data['error'])) throw new RuntimeException("Daemon error: " . $data['error']);
        
        return $data;
    }

    public function count(string $csvPath, array $where = []): int
    {
        $res = $this->query('count', ['csv' => $csvPath, 'where' => $where]);
        return (int)($res['count'] ?? 0);
    }

    private function ensureConnected(): void
    {
        if ($this->socket && is_resource($this->socket) && !feof($this->socket)) return;
        if (!file_exists($this->socketPath)) $this->startDaemon();
        $this->connect();
    }

    private function connect(): void
    {
        $this->socket = @stream_socket_client("unix://{$this->socketPath}", $errno, $errstr, 5);
        if (!$this->socket) throw new RuntimeException("Socket connect failed: $errstr");
    }

    private function startDaemon(): void
    {
        $cmd = escapeshellarg($this->binaryPath) . " daemon --socket " . escapeshellarg($this->socketPath);
        if ($this->indexDir) $cmd .= " --index-dir " . escapeshellarg($this->indexDir);
        $cmd .= " > /dev/null 2>&1 &";
        exec($cmd);

        $start = microtime(true);
        while (!file_exists($this->socketPath)) {
            if (microtime(true) - $start > 2) throw new RuntimeException("Daemon startup timeout");
            usleep(50000);
        }
    }

    public function __destruct()
    {
        if ($this->socket) fclose($this->socket);
    }
}
