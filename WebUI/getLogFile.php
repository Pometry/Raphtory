<?
function startsWith($haystack, $needle)
{
     $length = strlen($needle);
     return (substr($haystack, 0, $length) === $needle);
}



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

$cmd = "tail -$lines $log";
exec("$cmd 2>&1", $output);
echo "<table border=\"1\">";
foreach ($output as $line) {
	if(startsWith($line,'T')){
        echo "<tr><th>$line</th></tr>";
	}
	elseif(startsWith($line,'I')){
        echo "<tr><td>$line</td></tr>";
	}
	elseif(startsWith($line,'E')){
        echo "<tr><td>$line</td></tr>";
	}
}
echo "</table>";
?>
<!-- https://www.igotitworking.com/problem/view/71/ -->