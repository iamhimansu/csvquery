<?php
/**
 * CsvQuery - High-Performance CSV Query Engine
 *
 * @package CsvQuery
 * @author Himansu
 * @version 1.0.0
 */

declare(strict_types=1);

namespace iamhimansu\csvquery;

use InvalidArgumentException;
use RuntimeException;

/**
 * Main CsvQuery class - entry point for all operations.
 */
class CsvQuery
{
    /** @var string Path to CSV file */
    private string $csvPath;

    /** @var string Index directory */
    private string $indexDir;

    /** @var array CSV headers */
    private array $headers = [];

    /** @var array Column name to index mapping */
    private array $headerMap = [];

    /** @var string CSV separator */
    private string $separator;

    /** @var Executor Go binary wrapper */
    private Executor $executor;

    /** @var resource|null File handle for CSV */
    private $fileHandle = null;

    /**
     * Create a CsvQuery instance.
     *
     * @param string $csvPath Path to CSV file
     * @param array $options Configuration options:
     *   - 'indexDir': Directory for index files (default: same as CSV)
     *   - 'separator': CSV separator (default: ',')
     *   - 'binPath': Path to the Go binary
     *   - 'workers': Number of parallel workers for indexing
     */
    public function __construct(string $csvPath, array $options = [])
    {
        if (!file_exists($csvPath)) {
            throw new InvalidArgumentException("CSV file not found: $csvPath");
        }

        $this->csvPath = (string)realpath($csvPath);
        $this->indexDir = $options['indexDir'] ?? dirname($this->csvPath);
        $this->separator = $options['separator'] ?? ',';

        $this->executor = new Executor([
            'binaryPath' => $options['binPath'] ?? null,
            'workers'    => $options['workers'] ?? 0,
            'indexDir'   => $this->indexDir,
        ]);

        $this->readHeaders();
    }

    /**
     * Read CSV headers.
     */
    private function readHeaders(): void
    {
        $handle = fopen($this->csvPath, 'r');
        if (!$handle) {
            throw new RuntimeException("Could not open CSV file: {$this->csvPath}");
        }
        $line = fgets($handle);
        fclose($handle);

        if ($line === false) {
             throw new RuntimeException("CSV file is empty: {$this->csvPath}");
        }

        // Handle BOM and trim
        $line = preg_replace('/^\xEF\xBB\xBF/', '', trim($line));
        $this->headers = str_getcsv($line, $this->separator);
        $this->headerMap = array_flip($this->headers);
    }

    /**
     * Get CSV headers.
     *
     * @return array Column names
     */
    public function getHeaders(): array
    {
        return $this->headers;
    }

    /**
     * Create indexes for specified columns.
     *
     * @param array $columns Column names or composite columns (e.g., [['COL1', 'COL2']])
     * @return bool Success status
     */
    public function createIndex(array $columns): bool
    {
        return $this->executor->createIndex(
            $this->csvPath,
            $this->indexDir,
            json_encode($columns),
            $this->separator
        );
    }

    /**
     * Start a new query.
     *
     * @return QueryBuilder
     */
    public function find(): QueryBuilder
    {
        return new QueryBuilder($this);
    }

    /**
     * Start a WHERE query.
     *
     * @param array|string $column Column name or conditions
     * @param mixed $value Value to match (optional)
     * @return QueryBuilder
     */
    public function where($column, $value = null): QueryBuilder
    {
        return $this->find()->where($column, $value);
    }

    /**
     * Read a row at a specific offset.
     *
     * @param int $offset Byte offset
     * @return array|null Row data
     */
    public function readRowAt(int $offset): ?array
    {
        if ($this->fileHandle === null) {
            $this->fileHandle = fopen($this->csvPath, 'r');
        }

        fseek($this->fileHandle, $offset);
        $line = fgets($this->fileHandle);

        if ($line === false) {
            return null;
        }

        $values = str_getcsv(trim($line), $this->separator);
        
        if (count($this->headers) !== count($values)) {
             // Handle row mismatch or padding if needed
             return null;
        }

        return array_combine($this->headers, $values);
    }

    /**
     * Get the Go executor instance.
     */
    public function getExecutor(): Executor
    {
        return $this->executor;
    }

    /**
     * Get CSV file path.
     */
    public function getCsvPath(): string
    {
        return $this->csvPath;
    }

    /**
     * Get index directory.
     */
    public function getIndexDir(): string
    {
        return $this->indexDir;
    }

    /**
     * Get separator.
     */
    public function getSeparator(): string
    {
        return $this->separator;
    }

    /**
     * Get header map.
     */
    public function getHeaderMap(): array
    {
        return $this->headerMap;
    }

    public function __destruct()
    {
        if ($this->fileHandle !== null) {
            fclose($this->fileHandle);
        }
    }
}
