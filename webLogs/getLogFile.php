<?
if (isset($_REQUEST["log"])) {
        $log = trim($_REQUEST["log"]);
} else {
        echo "No log file specified.<br>";
        exit;
}

$lines = 20;
if (isset($_REQUEST["lines"])) {
        $lines = trim($_REQUEST["lines"]);
}

if (!file_exists($log)) {
        echo "$log does not exist.<br>";
        exit;
}

$cmd = "tail -$lines $log";
exec("$cmd 2>&1", $output);

foreach ($output as $line) {
        echo "$line<br>";
}
?>