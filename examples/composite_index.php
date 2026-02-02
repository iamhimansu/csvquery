<?php
/**
 * Composite Index Example
 * Shows how to use multi-column indexes for complex filters.
 */

require_once __DIR__ . '/../vendor/autoload.php';

use CsvQuery\CsvQuery;

$csvPath = __DIR__ . '/../tests/test_data.csv';
$csv = new CsvQuery($csvPath);

// Create a composite index
echo "Creating composite index on [YEAR, DEPARTMENT]...\n";
$csv->createIndex(['YEAR', 'DEPARTMENT']);

echo "Querying with multi-column filter...\n";
$start = microtime(true);

$results = $csv->find()
    ->where([
        'YEAR' => 2024,
        'DEPARTMENT' => 'Engineering'
    ])
    ->all();

$end = microtime(true);

echo "Found " . count($results) . " matching records in " . round(($end - $start) * 1000, 2) . "ms\n";
