<?php
/**
 * Real World Example: Student Exam Results
 * Demonstrates complex filtering, sorting, and aggregations.
 */

require_once __DIR__ . '/../vendor/autoload.php';

use iamhimansu\csvquery\CsvQuery;

$csvPath = __DIR__ . '/../tests/student_results.csv';
$csv = new CsvQuery($csvPath);

echo "--- Student Performance Report ---\n";

// 1. Get Top 5 Students in Computer Science
$topStudents = $csv->find()
    ->select(['NAME', 'SCORE', 'GRADE'])
    ->where(['SUBJECT' => 'Computer Science'])
    ->andWhere(['>=', 'SCORE', 90])
    ->orderBy(['SCORE' => SORT_DESC])
    ->limit(5)
    ->all();

echo "Top 5 CS Students:\n";
foreach ($topStudents as $i => $s) {
    echo ($i+1) . ". {$s['NAME']} - {$s['SCORE']} ({$s['GRADE']})\n";
}

// 2. Performance Summary by Year
echo "\nGrade Distribution (2024):\n";
$stats = $csv->find()
    ->where(['YEAR' => 2024])
    ->groupBy('GRADE')
    ->count();

foreach ($stats as $grade => $count) {
    echo "- {$grade}: {$count} students\n";
}

// 3. Average Score Calculation
$totalScore = $csv->find()->where(['YEAR' => 2024])->sum('SCORE');
$totalStudents = $csv->find()->where(['YEAR' => 2024])->count();

echo "\nAverage Score in 2024: " . round($totalScore / $totalStudents, 2) . "\n";
