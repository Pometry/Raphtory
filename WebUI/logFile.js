function getLog(log, lines) {
                var url = "getLogFile.php?log=" + log + "&lines=" + lines;
                request.open("GET", url, true);
                request.onreadystatechange = updatePage;
                request.send(null);
        }

        function tail(command,log,lines) {
                if (command == "start") {
                        window.alert("DDDD")
                        document.getElementById("watchStart").disabled = true;
                        document.getElementById("watchStop").disabled = false;
                        timer = setInterval(function() {getLog("index.php",20);},5000);
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