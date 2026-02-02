<?php
/**
 * Indexed Query Example
 * Demonstrates how to create and use indexes for extreme performance.
 */

require_once __DIR__ . '/../vendor/autoload.php';

use CsvQuery\CsvQuery;

$csvPath = __DIR__ . '/../tests/test_data.csv';
$csv = new CsvQuery($csvPath);

// 1. Create an index (only needs to be done once)
echo "Ensuring index on USER_ID exists...\n";
$csv->createIndex(['USER_ID']);

// 2. Perform an indexed lookup
echo "Searching for USER_ID 54321...\n";
$start = microtime(true);

$row = $csv->find()
    ->where(['USER_ID' => 54321])
    ->one();

$end = microtime(true);

if ($row) {
    echo "Match found: {$row['NAME']} (in " . round(($end - $start) * 1000, 2) . "ms)\n";
} else {
    echo "No match found.\n";
}

// 3. Compare with a non-indexed column (full scan)
echo "\nPerforming full scan on non-indexed column...\n";
$start = microtime(true);

$count = $csv->find()
    ->where(['BIO' => 'Software Engineer'])
    ->count();

$end = microtime(true);
echo "Counted {$count} rows in " . round(($end - $start) * 1000, 2) . "ms\n";
