# pipelines/run_tests.ps1
$ErrorActionPreference = "Stop"

$duckdb = ".\tools\duckdb\duckdb_cli-windows-amd64\duckdb.exe"
$db     = "data\processed\olist.duckdb"
$tests  = "tests\duckdb_test_report.sql"

& $duckdb $db -f $tests
Write-Host "Tests executed. If you saw no failing rows above, you're good."
