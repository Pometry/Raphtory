


<!DOCTYPE html>
<html>
<body>
        <script language="javascript" type="text/JavaScript">
                function getLog(log, lines) {
                        var url = "getLogFile.php?log=" + log + "&lines=" + lines;
                        request.open("GET", url, true);
                        request.onreadystatechange = updatePage;
                        request.send(null);
                }

                function tail(command) {
                        if (command == "start") {
                                document.getElementById("watchStart").disabled = true;
                                document.getElementById("watchStop").disabled = false;
                                timer = setInterval(function() {getLog("/Users/Mirate/Documents/PhD/project/docker-exp-cluster/logs/benchmark.txt",27);},5000);
                        } else {
                                document.getElementById("watchStart").disabled = false;
                                document.getElementById("watchStop").disabled = true;
                                clearTimeout(timer);
                        }
                }

                function updatePage() {
                        if (request.readyState == 4) {
                                if (request.status == 200) {
                                        var currentLogValue = request.responseText.split("\n");
                                        eval(currentLogValue);

                                        document.getElementById("log").innerHTML = currentLogValue;
                                }
                        }
                }

                var request = (window.XMLHttpRequest) ? new XMLHttpRequest() : (window.ActiveXObject ? new window.ActiveXObject("Microsoft.XMLHTTP") : false); 
        </script>

        <div id="log" style="width:100%; height:90%; overflow:auto;"></div>
        <table>
                <tr>
                        <td>
                                <input type="button" style="width:40px; 0px" id="watchStart" name="watch" value="Start" onclick="tail('start');">
                                <input type="button" style="width:40px; 0px" id="watchStop" name="watch" value="Stop" disabled=true onclick="tail('stop');">
                        </td>
                </tr>
        </table>
</body>
</html>