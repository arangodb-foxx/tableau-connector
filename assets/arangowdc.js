(function () {
  "use strict";
  /**@type {HTMLDivElement} */
  var queriesDiv = document.getElementById("queries");
  var errorsDiv = document.getElementById("errors");
  /**@type {HTMLTemplateElement} */
  var queryTemplate = document.getElementById("query");
  var addQueryBtn = document.getElementById("add-query");
  var form = document.forms[0];
  /**@type {string} */
  var connectionName;
  var queries = [];

  function extend(base, ext) {
    Object.keys(ext).forEach(function (key) {
      base[key] = ext[key];
    });
    return base;
  }

  function findIndex(arr, fn) {
    for (var i = 0; i < arr.length; i++) {
      var found = fn(arr[i]);
      if (found) return i;
    }
    return -1;
  }

  /**@type {<T>(maybeArr: T | ArrayLike<T>) => T[]} */
  function splat(maybeArr) {
    if (typeof maybeArr.length !== "number") return [maybeArr];
    var arr = [];
    for (var i = 0; i < maybeArr.length; i++) {
      arr[i] = maybeArr[i];
    }
    return arr;
  }

  function fetchJson(url, options) {
    return fetch(url, options).then(function (response) {
      if (response.status === 204) return response;
      return response.json().then(function (body) {
        response._body = body;
        return response;
      });
    });
  }

  function consumeCursorResult(body, table, schema, offset) {
    var count = offset + body.result.length;
    table.appendRows(
      body.result.map(function (row, i) {
        return extend(
          { _i: offset + i + 1 },
          Array.isArray(schema)
            ? row.reduce(function (obj, value, j) {
                obj["value" + j] = value;
                return obj;
              }, {})
            : typeof schema === "string"
            ? { value: row }
            : row
        );
      })
    );
    if (!body.hasMore) return count;
    var headers = new Headers();
    headers.append("authorization", getAuthorization());
    return fetchJson("../_api/cursor/" + body.id, {
      method: "PUT",
      headers: headers,
    }).then(function (response) {
      return consumeCursorResult(response._body, table, schema, count);
    });
  }

  function getAuthorization() {
    return "Basic " + btoa(tableau.username + ":" + tableau.password);
  }

  function fetchAllNonIncremental(query, table, schema) {
    console.log('Fetching all items for table "' + table.tableInfo.id + '"...');
    var headers = new Headers();
    headers.append("authorization", getAuthorization());
    return fetchJson("../_api/cursor", {
      method: "POST",
      headers: headers,
      body: JSON.stringify({
        query: query,
      }),
    }).then(function (response) {
      return consumeCursorResult(response._body, table, schema, 0);
    });
  }

  function fetchAllIncremental(query, table, schema, offset, fullCount) {
    if (!offset) offset = 0;
    console.log(
      "Fetching increment from " +
        offset +
        ' for table "' +
        table.tableInfo.id +
        '"...'
    );
    var headers = new Headers();
    headers.append("authorization", getAuthorization());
    return fetchJson("../_api/cursor", {
      method: "POST",
      headers: headers,
      body: JSON.stringify({
        query: query,
        bindVars: { OFFSET: offset },
        options: { fullCount: !fullCount },
      }),
    })
      .then(function (response) {
        fullCount = response._body.extra.stats.fullCount || fullCount;
        return consumeCursorResult(response._body, table, schema, offset);
      })
      .then(function (count) {
        if (count >= fullCount) return count;
        console.log("Found " + count + " so far...");
        return fetchAllIncremental(query, table, schema, count, fullCount);
      });
  }

  function fetchOneIncrement(query, table, schema, offset) {
    console.log(
      "Fetching one increment from " +
        offset +
        ' for table "' +
        table.tableInfo.id +
        '"...'
    );
    if (!offset) offset = 0;
    var headers = new Headers();
    headers.append("authorization", getAuthorization());
    return fetchJson("../_api/cursor", {
      method: "POST",
      headers: headers,
      body: JSON.stringify({
        query: query,
        bindVars: { OFFSET: offset },
      }),
    }).then(function (response) {
      return consumeCursorResult(response._body, table, schema, offset);
    });
  }

  function fetchData(query, table) {
    if (query.incremental) {
      var offset = parseFloat(table.incrementValue);
      console.log(
        'Increment value from Tableau is: "' + table.incrementValue + '"'
      );
      if (!isNaN(offset) && offset >= 0) {
        return fetchOneIncrement(
          query.query,
          table,
          query.schema,
          Math.max(0, offset - 1)
        ).then(function (count) {
          console.log(
            "Done fetching " +
              count +
              ' items for table "' +
              table.tableInfo.id +
              '".'
          );
        });
      }
      return fetchAllIncremental(query.query, table, query.schema, 0).then(
        function (count) {
          console.log(
            "Done fetching all " +
              count +
              ' items for table "' +
              table.tableInfo.id +
              '".'
          );
        }
      );
    } else {
      return fetchAllNonIncremental(query.query, table, query.schema).then(
        function (count) {
          console.log(
            "Done fetching all " +
              count +
              ' items for table "' +
              table.tableInfo.id +
              '".'
          );
        }
      );
    }
  }

  function inferSchema(result) {
    if (typeof result === "string") {
      if (result.match(/^\d{4}-\d{2}-\d{2}$/)) {
        return tableau.dataTypeEnum.date;
      }
      if (
        result.match(
          /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[-+]\d+(:\d+))?$/
        )
      ) {
        return tableau.dataTypeEnum.datetime;
      }
      return tableau.dataTypeEnum.string;
    }
    if (typeof result === "boolean") return tableau.dataTypeEnum.bool;
    if (typeof result === "number") return tableau.dataTypeEnum.float;
    if (Array.isArray(result)) return result.map(inferSchema);
    if (result && typeof result === "object") {
      return Object.keys(result).reduce(function (obj, key) {
        obj[key] = inferSchema(result[key]);
        return obj;
      }, {});
    }
    console.error("Can't infer schema for value \"" + result + '"!');
    return null;
  }

  function addQuery(opts) {
    if (!opts) {
      if (queriesDiv.children.length) {
        opts = { id: "", alias: "", query: "" };
      } else {
        opts = { id: "arangoWdc", alias: "ArangoDB WDC result set", query: "" };
      }
    }
    var query = queryTemplate.firstElementChild.cloneNode(true);
    var removeQueryBtn = query.querySelector("button");
    function remove() {
      removeQueryBtn.removeEventListener("click", remove);
      queriesDiv.removeChild(query);
    }
    removeQueryBtn.addEventListener("click", remove);
    query = queriesDiv.appendChild(query);
    query.querySelector('[name="queryId"]').value = opts.id;
    query.querySelector('[name="queryAlias"]').value = opts.alias;
    query.querySelector('[name="queryAql"]').value = opts.query;
    if (queriesDiv.children.length === 1) {
      query.querySelector('[name="queryAql"]').placeholder =
        "e.g. FOR doc IN documents LIMIT @OFFSET, 100 RETURN doc";
    }
  }

  addQueryBtn.addEventListener("click", function (evt) {
    evt.preventDefault();
    addQuery();
  });
  form.addEventListener("submit", function (evt) {
    evt.preventDefault();
    submit();
  });

  var connector = tableau.makeConnector();
  connector.init = function init(done) {
    if (tableau.connectionData) {
      try {
        var data = JSON.parse(tableau.connectionData);
        connectionName = data.connectionName || "ArangoDB WDC";
        queries = data.queries || [];
        form.username.value = tableau.username || "";
        form.password.value = tableau.password || "";
        form.dsn.value = connectionName;
        for (var i = 0; i < queries.length; i++) {
          addQuery(queries[i]);
        }
      } catch (e) {
        console.error(e);
      }
    } else {
      addQuery();
    }
    done();
  };
  connector.getSchema = function getSchema(done) {
    done(
      queries.map(function (query) {
        console.log(
          'Query "' +
            query.id +
            '" is ' +
            (query.incremental ? "" : "not ") +
            "incremental"
        );
        return {
          id: query.id,
          alias: query.alias,
          incrementColumnId: query.incremental ? "_i" : undefined,
          columns: [].concat.apply(
            [{ id: "_i", alias: "index", dataType: tableau.dataTypeEnum.int }],
            Array.isArray(query.schema)
              ? query.schema.map(function (schema, i) {
                  return {
                    id: "value" + i,
                    dataType: schema,
                  };
                })
              : typeof query.schema === "string"
              ? [{ id: "value", dataType: query.schema }]
              : Object.keys(query.schema).map(function (key) {
                  return {
                    id: key,
                    dataType: query.schema[key],
                  };
                })
          ),
        };
      })
    );
  };
  connector.getData = function getData(table, done) {
    var i = findIndex(queries, function (query) {
      return query.id === table.tableInfo.id;
    });
    console.log('Fetching results for table "' + table.tableInfo.id + '" ...');
    return fetchData(queries[i], table).then(done, function (error) {
      tableau.abortWithError(error.stack || String(error));
    });
  };

  tableau.registerConnector(connector);

  function submit() {
    tableau.username = form.username.value;
    tableau.password = form.password.value;
    connectionName = form.dsn.value || "ArangoDB WDC";
    queries = [];
    var queryAql = splat(form.queryAql).map(function (el) {
      return el.value;
    });
    var queryId = splat(form.queryId).map(function (el) {
      return el.value;
    });
    var queryAlias = splat(form.queryAlias).map(function (el) {
      return el.value;
    });
    var headers = new Headers();
    headers.append("authorization", getAuthorization());
    return fetch("../_api/version", { headers: headers })
      .then(function (response) {
        if (!response.ok) {
          throw new Error("Invalid credentials");
        }
        return queryAql.reduce(function (p, _, i) {
          return p.then(function () {
            console.log("Parsing query #" + (i + 1) + " ...");
            return fetchJson("../_api/query", {
              method: "POST",
              headers: headers,
              body: JSON.stringify({ query: queryAql[i] }),
            }).then(function (response) {
              if (!response.ok) {
                throw new Error(
                  "Invalid query #" +
                    (i + 1) +
                    ": " +
                    response._body.errorMessage
                );
              }
              if (
                response._body.bindVars.length &&
                (response._body.bindVars.length > 1 ||
                  response._body.bindVars[0] !== "OFFSET")
              ) {
                throw new Error(
                  "Invalid bindVars in query #" +
                    (i + 1) +
                    ": " +
                    response._body.bindVars.join(", ") +
                    " (only @OFFSET allowed)"
                );
              }
              queries.push({
                id: queryId[i],
                alias: queryAlias[i],
                query: queryAql[i],
                incremental: response._body.bindVars[0] === "OFFSET",
              });
            });
          });
        }, Promise.resolve());
      })
      .then(function () {
        return queries.reduce(function (p, _, i) {
          return p.then(function () {
            console.log("Inferring schema for query #" + i + " ...");
            var query = queries[i].query;
            var incremental = queries[i].incremental;
            return fetch("../_api/cursor", {
              method: "POST",
              headers: headers,
              body: JSON.stringify({
                query: query,
                bindVars: incremental ? { OFFSET: 0 } : undefined,
                batchSize: 1,
              }),
            })
              .then(function (response) {
                return response.json().then(function (body) {
                  response._body = body;
                  return response;
                });
              })
              .then(function (response) {
                queries[i].schema = inferSchema(response._body.result[0]);
                if (response._body.id) {
                  console.log("Disposing of cursor ...");
                  return fetch("../_api/cursor/" + response._body.id, {
                    method: "DELETE",
                    headers: headers,
                  });
                }
              });
          });
        }, Promise.resolve());
      })
      .then(function () {
        tableau.connectionData = JSON.stringify({
          connectionName: connectionName,
          queries: queries,
        });
        tableau.connectionName = connectionName;
        tableau.submit();
      })
      .catch(function (error) {
        errorsDiv.innerHTML = error.message;
      });
  }
})();
