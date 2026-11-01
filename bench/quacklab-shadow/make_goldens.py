#!/usr/bin/env python3
"""Generate corpora/goldens.jsonl from grain's shared golden fixtures.

Each fixture (fixtures/shared/goldens/*.json) becomes a run of /sql requests:
drop the fixture's schemas, its setup (statement extraction and setup come
from tools/quacklab-goldens.py: CREATE SCHEMA, tables with column names,
types, NOT NULL and DEFAULT, then views in dependency order), then every
statement the fixture carries — each table/plan's selectSql first, then the
SQL strings found in runtimeSqlCatalog, executionPlan and sqlArtifacts.

Two rewrites make one session serve all fixtures:

  * `r2_cat.` becomes `warehouse.`. QuackLab's file mode opens the warehouse
    file, so DuckDB names that catalog after the file (`warehouse`); pinkdb's
    adapter serves the `warehouse` database. Both sides then run the same SQL
    with no ATTACH/CREATE DATABASE dance, which is the point of a shadow run.
  * `read_csv('s3://$rawBucket/$sourcePath', ...)` becomes a local gzip CSV,
    and $batchId becomes batch-1 (same as the reference tool).

A query without a top-level ORDER BY is marked "unordered": row order is not
part of the contract and hash aggregation order differs between engines.

    python3 bench/quacklab-shadow/make_goldens.py            # regenerate
    python3 bench/quacklab-shadow/make_goldens.py --fixtures model_shared_view_id_collision

Stdlib only. The regenerated log is what shadow.py replays.
"""

import argparse
import glob
import gzip
import hashlib
import json
import os
import re

TY = {"string": "VARCHAR", "timestamp": "TIMESTAMP", "number": "DOUBLE",
      "integer": "BIGINT", "boolean": "BOOLEAN"}
STATEMENT = re.compile(
    r"\s*(WITH|SELECT|INSERT|CREATE|DELETE|UPDATE|MERGE|DROP|ALTER|TRUNCATE|COPY|CALL)\b",
    re.I)
ORDER_BY = re.compile(r"\border\s+by\b", re.I)
CSV_SOURCE = re.compile(
    r"SELECT ((?:\"[^\"]+\",? ?)+) FROM read_csv\('s3://\$rawBucket/\$sourcePath'")


def quote(ident):
    return '"' + ident.replace('"', '""') + '"'


def literal(value):
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if isinstance(value, bool):
        return str(value).upper()
    return str(value)


def setup(specs):
    """CREATE SCHEMA / TABLE / VIEW for one fixture (engine: duck flavours)."""
    statements = []
    for schema in sorted({t["schema"] for t in specs}):
        # The log is one session, so the reset above may not have removed a
        # schema a previous corpus left behind.
        statements.append("CREATE SCHEMA IF NOT EXISTS " + quote(schema))
    for table in specs:
        lifecycle = table["lifecycle"].get("materialization")
        if lifecycle in ("view", "materialized_view") and table.get("selectSql"):
            continue
        columns = []
        for column in table["columns"]:
            definition = quote(column["name"]) + " " + TY.get(column["dataType"], "VARCHAR")
            if not column.get("nullable", True):
                definition += " NOT NULL"
            if "default" in column:
                definition += " DEFAULT " + literal(column["default"])
            columns.append(definition)
        statements.append("CREATE TABLE %s.%s (%s)" % (
            quote(table["schema"]), quote(table["table"]), ", ".join(columns)))
    views = {t["id"]: t for t in specs
             if t["lifecycle"].get("materialization") in ("view", "materialized_view")
             and t.get("selectSql")}
    done = set()

    def emit(table_id):
        if table_id in done or table_id not in views:
            return
        done.add(table_id)
        for dependency in views[table_id].get("dependsOn", []):
            emit(dependency)
        view = views[table_id]
        statements.append("CREATE VIEW %s.%s AS %s" % (
            quote(view["schema"]), quote(view["table"]), view["selectSql"]))

    for table_id in views:
        emit(table_id)
    return statements


def csv_file(match):
    """Materialize a read_csv source as a local gzip CSV (as the reference did)."""
    columns = re.findall(r'"([^"]+)"', match.group(1))
    digest = hashlib.md5(",".join(columns).encode()).hexdigest()[:8]
    os.makedirs("/tmp/qlaudit", exist_ok=True)
    path = "/tmp/qlaudit/src_%s.csv.gz" % digest
    if not os.path.exists(path):
        with gzip.open(path, "wt") as handle:
            handle.write(",".join(columns) + "\n")
            handle.write(",".join("v%d" % i for i in range(len(columns))) + "\n")
    return match.group(0).replace("s3://$rawBucket/$sourcePath", path)


def prepare(sql):
    sql = sql.replace("$batchId", "batch-1")
    sql = CSV_SOURCE.sub(csv_file, sql)
    return sql.replace("r2_cat.", "warehouse.")


def cases_of(document, specs):
    cases = []
    for table in specs:
        if table.get("selectSql"):
            cases.append(("selectSql:" + table["id"], table["selectSql"]))

    def walk(node, path):
        if isinstance(node, dict):
            for key, value in node.items():
                walk(value, path + "." + key)
        elif isinstance(node, list):
            for i, value in enumerate(node):
                walk(value, path + "[%d]" % i)
        elif (isinstance(node, str) and len(node) > 15
              and STATEMENT.match(node)):
            cases.append((path, node))

    walk(document["physicalArtifacts"]["runtimeSqlCatalog"], "runtime")
    walk(document["executionPlan"], "plan")
    walk(document["sqlArtifacts"], "artifacts")
    return cases


def request(sql, note, unordered=False):
    return {"path": "/sql", "method": "POST", "body": {"sql": sql},
            "auth": "token", "unordered": unordered, "note": note}


def requests_of(fixture_path):
    with open(fixture_path) as handle:
        document = json.load(handle)
    name = os.path.basename(fixture_path)[:-len(".json")]
    specs = document["physicalArtifacts"]["tableSpecs"]
    schema_set = sorted({t["schema"] for t in specs})
    requests = [request("DROP SCHEMA IF EXISTS %s CASCADE" % quote(schema),
                        "reset %s (%s)" % (schema, name))
                for schema in schema_set]
    for sql in setup(specs):
        requests.append(request(prepare(sql), "setup " + name))
    seen = set()
    for path, sql in cases_of(document, specs):
        if sql in seen:
            continue
        seen.add(sql)
        stripped = sql.strip().rstrip(";")
        unordered = bool(not ORDER_BY.search(stripped)
                         and re.match(r"\s*(SELECT|WITH)\b", stripped, re.I))
        requests.append(request(prepare(stripped), name + " " + path, unordered))
    return name, requests


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--grain", default=os.path.expanduser(
        "~/Desktop/projects/grain/grain"), help="grain checkout")
    parser.add_argument("--out", default=os.path.join(here, "corpora", "goldens.jsonl"),
                        help="output request log")
    parser.add_argument("--fixtures", nargs="*", metavar="NAME",
                        help="only these fixture names (default: all)")
    args = parser.parse_args()

    paths = sorted(glob.glob(os.path.join(
        args.grain, "fixtures", "shared", "goldens", "*.json")))
    if args.fixtures:
        wanted = set(args.fixtures)
        paths = [p for p in paths if os.path.basename(p)[:-5] in wanted]
    if not paths:
        raise SystemExit("no golden fixtures found under " + args.grain)

    total = 0
    with open(args.out, "w") as out:
        for path in paths:
            name, requests = requests_of(path)
            for entry in requests:
                out.write(json.dumps(entry) + "\n")
            print("%-46s %3d requests" % (name, len(requests)))
            total += len(requests)
    print("%-46s %3d requests -> %s" % (os.path.basename(args.out), total, args.out))


if __name__ == "__main__":
    main()
