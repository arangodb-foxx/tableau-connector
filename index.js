"use strict";
const path = require("path");
const createRouter = require("@arangodb/foxx/router");
const { context } = require("@arangodb/locals");
const router = createRouter();
context.use(router);

const assets = context.fileName("assets");

router.get("/", (req, res) => {
  res.redirect(302, req.makeAbsolute("index.html"));
});

router.get("*", (req, res) => {
  const filename = path.normalize(path.sep + req.suffix).slice(1);
  res.sendFile(path.resolve(assets, filename));
});
