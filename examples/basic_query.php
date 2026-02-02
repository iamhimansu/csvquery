<?php

spl_autoload_register(function ($class) {
    $prefix = 'CsvQuery\\';
    $base_dir = __DIR__ . '/../src/CsvQuery/';
    $len = strlen($prefix);
    if (strncmp($prefix, $class, $len) !== 0) return;
    $relative_class = substr($class, $len);
    $file = $base_dir . str_replace('\\', '/', $relative_class) . '.php';
    if (file_exists($file)) {
        require $file;
    }
});

use CsvQuery\Config;
use CsvQuery\Client;
use CsvQuery\Executor;

// Setup
$config = new Config(__DIR__ . '/data.csv');
$client = new Client($config);
$executor = new Executor($client);

// 1. Indexing (Optional but recommended)
echo "Indexing...\n";
try {
    $executor->index(['department', 'role']);
    echo "Index created.\n";
} catch (\Exception $e) {
    echo "Indexing failed (or already exists): " . $e->getMessage() . "\n";
}

// 2. Querying
echo "\nQuerying Engineers...\n";
$result = $executor->query()
    ->where('department', 'Engineering')
    ->get(); // Now returns Result object

echo "Found " . $result->count() . " records:\n";
foreach ($result as $row) { // Iterating returns Rows
    // $row is now ['offset', 'line']
    // In a full implementation, Result might fetch actual data.
    // For now, it returns offsets.
    echo " - Row at line " . $row['line'] . "\n";
}

// 3. Counting
echo "\nCounting Sales...\n";
$count = $executor->query()
    ->where('department', 'Sales')
    ->count();
echo "Sales count: $count\n";

// 4. Aggregation
echo "\nAvg Salary by Department...\n";
// Note: Aggregation requires loading actual data which Executor does if config matches.
// Our current Go implementation handles aggregation if GroupBy is set.
$aggResult = $executor->query()
    ->groupBy('department')
    ->aggregate('avg', 'salary')
    ->get(); // Result object with groups

$groups = $aggResult->getGroups(); // Access raw group data
var_dump($aggResult->toArray());
if ($groups) {
    print_r($groups);
} else {
    echo "No aggregation results.\n";
}
