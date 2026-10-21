/* pinkdb console — self-hosted script.
   No framework, no import, no external resource: the page talks to the
   console's own JSON API. Cell values are inserted with textContent (never
   innerHTML), so a cell that contains markup is shown as text, never
   executed. */

(function () {
  "use strict";

  var state = {
    db: null,
    table: null,
    page: 1,
    pageSize: 50,
    token: ""
  };

  function $(id) {
    return document.getElementById(id);
  }

  function headers() {
    var head = {};
    if (state.token) {
      head["Authorization"] = "Bearer " + state.token;
    }
    return head;
  }

  function api(path) {
    return fetch(path, { headers: headers(), credentials: "same-origin" }).then(
      function (response) {
        if (!response.ok) {
          return response.json().then(
            function (body) {
              throw new Error(body && body.error ? body.error : "HTTP " + response.status);
            },
            function () {
              throw new Error("HTTP " + response.status);
            }
          );
        }
        return response.json();
      }
    );
  }

  function clear(node) {
    while (node.firstChild) {
      node.removeChild(node.firstChild);
    }
  }

  function text(tag, value, className) {
    var el = document.createElement(tag);
    el.textContent = value;
    if (className) {
      el.className = className;
    }
    return el;
  }

  function status(message) {
    var node = $("status");
    if (node) {
      node.textContent = message;
    }
  }

  function listItem(label, onClick) {
    var li = document.createElement("li");
    li.textContent = label;
    li.addEventListener("click", onClick);
    return li;
  }

  function loadDatabases() {
    api("/api/databases")
      .then(function (data) {
        var list = $("databases");
        clear(list);
        data.databases.forEach(function (db) {
          var item = listItem(db.name + (db.current ? " (current)" : ""), function () {
            selectDatabase(db.name);
          });
          if (db.current) {
            item.className = "active";
          }
          list.appendChild(item);
        });
        if (!state.db && data.current) {
          selectDatabase(data.current);
        } else if (state.db) {
          selectDatabase(state.db);
        }
      })
      .catch(function (err) {
        status("error: " + err.message);
      });
  }

  function selectDatabase(name) {
    state.db = name;
    state.table = null;
    status("loading " + name + "…");
    api("/api/tables?db=" + encodeURIComponent(name))
      .then(function (data) {
        var list = $("tables");
        clear(list);
        data.tables.forEach(function (table) {
          list.appendChild(
            listItem(table.name + " (" + table.columns.length + " cols)", function () {
              selectTable(table.name);
            })
          );
        });
        loadStats(name);
      })
      .catch(function (err) {
        status("error: " + err.message);
      });
  }

  function loadStats(db) {
    api("/api/stats?db=" + encodeURIComponent(db))
      .then(function (data) {
        var box = $("stats");
        clear(box);
        data.tables.forEach(function (table) {
          var stat = text("span", "", "stat");
          stat.appendChild(text("b", table.rows.toLocaleString()));
          stat.appendChild(document.createTextNode(" rows · "));
          stat.appendChild(text("b", table.name));
          stat.appendChild(
            document.createTextNode(" · ~" + formatBytes(table.estimated_bytes))
          );
          box.appendChild(stat);
        });
      })
      .catch(function () {
        /* the stats panel is optional */
      });
  }

  function formatBytes(bytes) {
    if (bytes < 1024) {
      return bytes + " B";
    }
    if (bytes < 1024 * 1024) {
      return (bytes / 1024).toFixed(1) + " KiB";
    }
    return (bytes / (1024 * 1024)).toFixed(1) + " MiB";
  }

  function selectTable(name) {
    state.table = name;
    state.page = 1;
    loadRows();
  }

  function loadRows() {
    if (!state.db || !state.table) {
      return;
    }
    status("loading " + state.db + "." + state.table + "…");
    var path =
      "/api/rows?db=" +
      encodeURIComponent(state.db) +
      "&table=" +
      encodeURIComponent(state.table) +
      "&page=" +
      state.page +
      "&page_size=" +
      state.pageSize;
    api(path)
      .then(function (data) {
        $("title").textContent = data.db + "." + data.table;
        renderGrid(data);
        renderPager(data);
        status(
          data.total_rows.toLocaleString() +
            " rows · page " +
            data.page +
            " of " +
            Math.max(data.total_pages, 1)
        );
      })
      .catch(function (err) {
        status("error: " + err.message);
      });
  }

  function renderGrid(data) {
    var host = $("preview");
    clear(host);
    var table = document.createElement("table");
    table.className = "grid";

    var head = document.createElement("thead");
    var names = document.createElement("tr");
    data.columns.forEach(function (column) {
      names.appendChild(text("th", column.name));
    });
    head.appendChild(names);
    var types = document.createElement("tr");
    types.className = "types";
    data.columns.forEach(function (column) {
      types.appendChild(text("th", column.type));
    });
    head.appendChild(types);
    table.appendChild(head);

    var body = document.createElement("tbody");
    data.rows.forEach(function (row) {
      var tr = document.createElement("tr");
      row.forEach(function (cell) {
        var td = text("td", cell.text, cell.null ? "null" : "");
        if (cell.truncated) {
          td.className = "truncated";
          td.title = cell.text;
        }
        tr.appendChild(td);
      });
      body.appendChild(tr);
    });
    table.appendChild(body);
    host.appendChild(table);
  }

  function renderPager(data) {
    var host = $("pager");
    clear(host);
    if (data.page > 1) {
      var prev = text("a", "previous", "button");
      prev.href = "#";
      prev.addEventListener("click", function (event) {
        event.preventDefault();
        state.page = data.page - 1;
        loadRows();
      });
      host.appendChild(prev);
    }
    host.appendChild(
      text("span", "page " + data.page + " / " + Math.max(data.total_pages, 1))
    );
    if (data.page < data.total_pages) {
      var next = text("a", "next", "button");
      next.href = "#";
      next.addEventListener("click", function (event) {
        event.preventDefault();
        state.page = data.page + 1;
        loadRows();
      });
      host.appendChild(next);
    }
  }

  function init() {
    var token = $("token");
    if (token) {
      token.addEventListener("change", function () {
        state.token = token.value;
        loadDatabases();
      });
    }
    loadDatabases();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
