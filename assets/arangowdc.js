(function() {
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
    Object.keys(ext).forEach(function(key) {
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
      return Object.keys(result).reduce(function(obj, key) {
        obj[key] = inferSchema(result[key]);
        return obj;
      }, {});
    }
    console.error("Can't infer schema for value \"" + result + '"!');
    return null;
  }

  function addQuery() {
    var query = queryTemplate.firstElementChild.cloneNode(true);
    var removeQueryBtn = query.querySelector("button");
    function remove() {
      removeQueryBtn.removeEventListener("click", remove);
      queriesDiv.removeChild(query);
    }
    removeQueryBtn.addEventListener("click", remove);
    query = queriesDiv.appendChild(query);
    if (queriesDiv.children.length === 1) {
      query.querySelector('[name="queryId"]').value = "arangoWdc";
      query.querySelector('[name="queryAlias"]').value =
        "ArangoDB WDC result set";
      query.querySelector('[name="queryAql"]').placeholder =
        "e.g. FOR doc IN documents LIMIT @OFFSET, 100 RETURN doc";
    }
  }

  addQuery();
  addQueryBtn.addEventListener("click", function(evt) {
    evt.preventDefault();
    addQuery();
  });
  form.addEventListener("submit", function(evt) {
    evt.preventDefault();
    submit();
  });

  var connector = tableau.makeConnector();
  connector.init = function init(done) {
    if (tableau.connectionData) {
      try {
        var data = JSON.parse(tableau.connectionData);
        connectionName = data.connectionName;
        queries = data.queries;
      } catch (e) {}
    }
    done();
  };
  connector.getSchema = function getSchema(done) {
    done(
      queries.map(function(query) {
        return {
          id: query.id,
          alias: query.alias,
          incrementColumnId: query.incremental ? "_i" : undefined,
          columns: [].concat.apply(
            [{ id: "_i", alias: "index", dataType: tableau.dataTypeEnum.int }],
            Array.isArray(query.schema)
              ? query.schema.map(function(schema, i) {
                  return {
                    id: "value" + i,
                    dataType: schema
                  };
                })
              : typeof query.schema === "string"
              ? [{ id: "value", dataType: query.schema }]
              : Object.keys(query.schema).map(function(key) {
                  return {
                    id: key,
                    dataType: query.schema[key]
                  };
                })
          )
        };
      })
    );
  };
  connector.getData = function getData(table, done) {
    var headers = new Headers();
    headers.append(
      "authorization",
      "Basic " + btoa(tableau.username + ":" + tableau.password)
    );
    var i = findIndex(queries, function(query) {
      return query.id === table.tableInfo.id;
    });
    var query = queries[i].query;
    var incremental = queries[i].incremental;
    var schema = queries[i].schema;
    console.log(table);
    var OFFSET = table.incrementValue ? Number(table.incrementValue) + 1 : 0;
    console.log('Fetching results for table "' + table.tableInfo.id + '" ...');
    if (incremental) console.log("with @OFFSET = " + OFFSET);

    fetch("../_api/cursor", {
      headers: headers,
      method: "POST",
      body: JSON.stringify({
        query: query,
        bindVars: incremental ? { OFFSET: OFFSET } : undefined
      })
    })
      .then(function(response) {
        return response.json().then(function(body) {
          response._body = body;
          return response;
        });
      })
      .then(function(response) {
        if (!response.ok) {
          throw new Error(response._body.errorMessage);
        }
        table.appendRows(
          response._body.result.map(function(row, i) {
            return extend(
              { _i: OFFSET + i },
              Array.isArray(schema)
                ? row.reduce(function(obj, value, i) {
                    obj["value" + i] = value;
                    return obj;
                  }, {})
                : typeof schema === "string"
                ? { value: row }
                : row
            );
          })
        );
        var cursorId = response._body.id;
        var offset = OFFSET + response._body.result.length;
        var hasMore = response._body.hasMore;
        function fetchMore() {
          if (!hasMore) return;
          console.log("Fetching more results ...");
          return fetch("../_api/cursor/" + cursorId, {
            headers: headers,
            method: "PUT"
          })
            .then(function(response) {
              return response.json().then(function(body) {
                response._body = body;
                return response;
              });
            })
            .then(function(response) {
              table.appendRows(
                response._body.result.map(function(row, i) {
                  return extend(
                    { _id: offset + i },
                    Array.isArray(schema)
                      ? row.reduce(function(obj, value, j) {
                          obj["value" + j] = value;
                          return obj;
                        }, {})
                      : typeof schema === "string"
                      ? { value: row }
                      : row
                  );
                })
              );
              offset += response._body.result.length;
              hasMore = response._body.hasMore;
              return fetchMore();
            });
        }
        return Promise.resolve().then(fetchMore);
      })
      .catch(function(error) {
        alert(error.message);
      })
      .finally(done);
  };

  tableau.registerConnector(connector);

  function submit() {
    tableau.username = form.username.value;
    tableau.password = form.password.value;
    connectionName = form.dsn.value || "ArangoDB WDC";
    queries = [];
    var queryAql = splat(form.queryAql).map(function(el) {
      return el.value;
    });
    var queryId = splat(form.queryId).map(function(el) {
      return el.value;
    });
    var queryAlias = splat(form.queryAlias).map(function(el) {
      return el.value;
    });
    var headers = new Headers();
    headers.append(
      "authorization",
      "Basic " + btoa(tableau.username + ":" + tableau.password)
    );
    return fetch("../_api/version", { headers: headers })
      .then(function(response) {
        if (!response.ok) {
          throw new Error("Invalid credentials");
        }
        return queryAql.reduce(function(p, _, i) {
          return p.then(function() {
            console.log("Parsing query #" + (i + 1) + " ...");
            return fetch("../_api/query", {
              headers: headers,
              method: "POST",
              body: JSON.stringify({ query: queryAql[i] })
            })
              .then(function(response) {
                return response.json().then(function(body) {
                  response._body = body;
                  return response;
                });
              })
              .then(function(response) {
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
                  incremental: response._body.bindVars[0] === "OFFSET"
                });
              });
          });
        }, Promise.resolve());
      })
      .then(function() {
        return queries.reduce(function(p, _, i) {
          return p.then(function() {
            console.log("Inferring schema for query #" + i + " ...");
            var query = queries[i].query;
            var incremental = queries[i].incremental;
            return fetch("../_api/cursor", {
              headers: headers,
              method: "POST",
              body: JSON.stringify({
                query: query,
                bindVars: incremental ? { OFFSET: 0 } : undefined,
                batchSize: 1
              })
            })
              .then(function(response) {
                return response.json().then(function(body) {
                  response._body = body;
                  return response;
                });
              })
              .then(function(response) {
                queries[i].schema = inferSchema(response._body.result[0]);
                if (response._body.id) {
                  console.log("Disposing of cursor ...");
                  return fetch("../_api/cursor/" + response._body.id, {
                    headers: headers,
                    method: "DELETE"
                  });
                }
              });
          });
        }, Promise.resolve());
      })
      .then(function() {
        tableau.connectionData = JSON.stringify({
          connectionName: connectionName,
          queries: queries
        });
        tableau.connectionName = connectionName;
        tableau.submit();
      })
      .catch(function(error) {
        errorsDiv.innerHTML = error.message;
      });
  }
})();
