// Opens YASGUI on the SPARQL endpoint of `graph`, with one tab per [name, view, query, endpoint suffix].
// The Raphtory server is http://localhost:1736 unless the page URL has ?server=...
function openYasgui(graph, prefixes, tabs) {
  const server = new URLSearchParams(location.search).get("server") || "http://localhost:1736";
  const endpoint = `${server}/sparql/${graph}`;
  document.getElementById("endpoint").textContent = endpoint;
  for (const link of document.querySelectorAll("header a[href]")) {
    link.search = location.search;
  }
  const yasgui = new Yasgui(document.getElementById("yasgui"), {
    persistenceId: null,
    copyEndpointOnNewTab: false,
    requestConfig: { endpoint, method: "POST" },
  });
  const first = yasgui.getTab();
  for (const [name, view, query, suffix] of tabs) {
    yasgui.addTab(false, {
      name,
      yasqe: { value: prefixes + query },
      yasr: { settings: { selectedPlugin: view, pluginsConfig: {} } },
      requestConfig: { endpoint: endpoint + (suffix || ""), acceptHeaderGraph: "text/turtle,*/*;q=0.9" },
    });
  }
  yasgui.selectTabId(yasgui.persistentConfig.getTabs()[1]);
  first.close();
  // available in the browser console
  window.yasgui = yasgui;
  return yasgui;
}
