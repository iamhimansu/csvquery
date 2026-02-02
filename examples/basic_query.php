<?php
/**
 * Basic Query Example
 * Shows how to load a CSV and perform a simple query.
 */

require_once __DIR__ . '/../vendor/autoload.php';

use iamhimansu\csvquery\CsvQuery;

$csvPath = __DIR__ . '/../tests/test_data.csv';

// Initialize
$csv = new CsvQuery($csvPath);

echo "Executing query...\n";
$start = microtime(true);

// Find all rows where STATUS is 'ACTIVE'
$results = $csv->find()
    ->where(['STATUS' => 'ACTIVE'])
    ->limit(5)
    ->all();

$end = microtime(true);

echo "Found " . count($results) . " rows in " . round(($end - $start) * 1000, 2) . "ms\n\n";

foreach ($results as $row) {
    echo "ID: {$row['ID']} | Name: {$row['NAME']} | Status: {$row['STATUS']}\n";
}
