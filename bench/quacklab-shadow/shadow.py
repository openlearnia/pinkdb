#!/usr/bin/env python3
"""Shadow run: replay one ordered request log against the Go QuackLab service
(DuckDB) and pinkdb's adapter, and diff the answers request by request.

The log is JSON lines, one request per line:

    {"path": "/sql", "method": "POST", "body": {"sql": "..."},
     "auth": "token" | "none", "unordered": false, "note": "..."}

Both sides get the same requests in the same order, so state built by an
earlier request is visible to a later one. A lease id returned by an acquire
(or renew) is substituted into later requests: any JSON string equal to
"$lease" becomes that side's own lease id, which keeps the two runs
independent.

Compared per request: HTTP status, the error code, the DuckDB error prefix
("Catalog Error:", "Binder Error:", ... — not the whole message, whose text
differs), and the result fields columns, rows, rowCount, truncated and
rowsAffected. Rows compare by value and JSON type (1, 1.0 and "1" are three
different values), in order unless the request is marked "unordered".
Timing fields are ignored, and so is anything the protocol does not pin
(request ids, intermediate timings).

    python3 bench/quacklab-shadow/shadow.py --ref http://127.0.0.1:12940 \\
        --pink http://127.0.0.1:1294 bench/quacklab-shadow/corpora/types.jsonl

Both sides are expected to use the token `tok` (override with --token).
--only N reruns request N (the earlier requests are still replayed: they are
its state), --from N reruns from N on. Exit code: 0 iff every request matches.
"""

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.request

# Result fields the protocol pins. Everything else in a successful body is
# timing or diagnostic and is ignored.
RESULT_FIELDS = ("columns", "rows", "rowCount", "truncated", "rowsAffected")

# DuckDB prefixes the message with the error kind; that kind is the part both
# services promise, the rest of the text is engine wording.
DUCK_ERROR = re.compile(r"([A-Z][A-Za-z]*(?: [A-Za-z]+)* Error:)")


def duck_error_prefix(message):
    m = DUCK_ERROR.search(message or "")
    return m.group(1) if m else None


def same_value(a, b):
    """JSON-equal: same type (int/float/str/bool/null/list/object) and value."""
    if type(a) is not type(b):
        return False
    if isinstance(a, list):
        return len(a) == len(b) and all(same_value(x, y) for x, y in zip(a, b))
    if isinstance(a, dict):
        return a.keys() == b.keys() and all(same_value(a[k], b[k]) for k in a)
    return a == b


def same_rows(a, b, unordered):
    if unordered:
        key = lambda row: json.dumps(row, sort_keys=True)
        a, b = sorted(a, key=key), sorted(b, key=key)
    return same_value(a, b)


def substitute(value, lease):
    """Replace the literal placeholder with a captured lease id, recursively."""
    if isinstance(value, dict):
        return {k: substitute(v, lease) for k, v in value.items()}
    if isinstance(value, list):
        return [substitute(v, lease) for v in value]
    return lease if value == "$lease" else value


def post(base, req, token, timeout):
    """One request. Returns (status, parsed body or None, transport error)."""
    url = base.rstrip("/") + req["path"]
    body = req.get("body")
    data = None if body is None else json.dumps(body).encode()
    call = urllib.request.Request(url, data=data, method=req.get("method", "POST"))
    if data is not None:
        call.add_header("Content-Type", "application/json")
    if req.get("auth") == "token":
        call.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(call, timeout=timeout) as resp:
            status, raw = resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        status, raw = exc.code, exc.read()
    except OSError as exc:
        return None, None, str(exc)
    text = raw.decode("utf-8", "replace")
    try:
        return status, json.loads(text), None
    except ValueError:
        return status, None, "non-JSON body: " + text[:120]


def replay(log, base, token, timeout, limit=None):
    """Replay the log against one service; returns per-request records."""
    out, lease = [], None
    for req in log[:limit]:
        body = req.get("body")
        if lease is None and body is not None and "$lease" in json.dumps(body):
            out.append({"status": None, "body": None,
                        "error": "no lease captured for $lease"})
            continue
        sent = dict(req, body=substitute(body, lease)) if body is not None else req
        started = time.monotonic()
        status, answer, error = post(base, sent, token, timeout)
        elapsed = (time.monotonic() - started) * 1000
        if "/write-lease/" in req["path"] and answer and "leaseId" in answer:
            lease = answer["leaseId"]
        out.append({"status": status, "body": answer, "error": error,
                    "ms": elapsed})
    return out


def diff_one(req, ref, pink):
    """Field-level differences for one replayed request; empty means match."""
    if ref["error"] or pink["error"]:
        got = "ref: %s / pink: %s" % (ref["error"] or ref["status"],
                                     pink["error"] or pink["status"])
        return ["transport: " + got]
    problems = []
    if ref["status"] != pink["status"]:
        problems.append("status: %s vs %s" % (ref["status"], pink["status"]))
    rb, pb = ref["body"], pink["body"]
    if rb is None or pb is None:
        if (rb is None) != (pb is None):
            problems.append("body: one side is not JSON")
        return problems
    re_, pe = rb.get("error"), pb.get("error")
    if re_ or pe:
        if not (re_ and pe):
            problems.append("error: %s vs %s" % (
                json.dumps(re_)[:120] if re_ else "none",
                json.dumps(pe)[:120] if pe else "none"))
            return problems
        if re_.get("code") != pe.get("code"):
            problems.append("error.code: %s vs %s"
                            % (re_.get("code"), pe.get("code")))
        rd = duck_error_prefix(re_.get("message"))
        pd = duck_error_prefix(pe.get("message"))
        if rd != pd:
            problems.append("error prefix: %s vs %s" % (rd, pd))
        return problems
    for field in RESULT_FIELDS:
        has_r, has_p = field in rb, field in pb
        if has_r != has_p:
            problems.append("%s: present on %s only"
                            % (field, "ref" if has_r else "pink"))
        elif not has_r:
            continue
        elif field == "rows":
            if not same_rows(rb[field], pb[field], req.get("unordered")):
                problems.append("rows: %d vs %d rows; %s"
                                % (len(rb[field]), len(pb[field]),
                                   first_row_diff(rb[field], pb[field])))
        elif not same_value(rb[field], pb[field]):
            problems.append("%s: %s vs %s"
                            % (field, show(rb[field]), show(pb[field])))
    return problems


def first_row_diff(ref, pink):
    """A short pointer to the first row that differs (for the report)."""
    if len(ref) != len(pink):
        return "row counts differ"
    for i, (a, b) in enumerate(zip(ref, pink)):
        if not same_value(a, b):
            return "row %d: %s vs %s" % (i, show(a), show(b))
    return "row order differs"


def show(value, width=160):
    text = json.dumps(value, separators=(",", ":"))
    return text if len(text) <= width else text[:width] + "…"


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("log", help="request log (JSON lines)")
    parser.add_argument("--ref", required=True, help="Go service base URL")
    parser.add_argument("--pink", required=True, help="pinkdb adapter base URL")
    parser.add_argument("--token", default="tok", help="SQL token (default tok)")
    parser.add_argument("--timeout", type=float, default=60.0,
                        help="per-request timeout in seconds (default 60)")
    parser.add_argument("--only", type=int, metavar="N",
                        help="compare request N only (1-based; prefix still replayed)")
    parser.add_argument("--from", dest="from_", type=int, metavar="N",
                        help="compare requests N.. (1-based)")
    parser.add_argument("--verbose", action="store_true",
                        help="print a row for every request, not only differences")
    args = parser.parse_args()

    with open(args.log) as handle:
        log = [json.loads(line) for line in handle if line.strip()]
    if not log:
        sys.exit("%s: empty request log" % args.log)
    first = args.only or args.from_ or 1
    if not 1 <= first <= len(log):
        sys.exit("%s: request %d is outside the log (1..%d)"
                 % (args.log, first, len(log)))
    limit = args.only if args.only else None

    started = time.monotonic()
    ref = replay(log, args.ref, args.token, args.timeout, limit)
    pink = replay(log, args.pink, args.token, args.timeout, limit)
    replay_ms = (time.monotonic() - started) * 1000 / 2

    rows, failed = [], 0
    compared = [i for i in range(1, len(log) + 1)
                if (not args.only or i == args.only)
                and (not args.from_ or i >= args.from_)]
    for i in compared:
        problems = diff_one(log[i - 1], ref[i - 1], pink[i - 1])
        if problems:
            failed += 1
        rows.append((i, log[i - 1], ref[i - 1], pink[i - 1], problems))

    print("%s: %d requests against %s and %s" % (
        args.log.rsplit("/", 1)[-1], len(compared), args.ref, args.pink))
    print("  #  ref  pink  request" + (" " * 30) + "result")
    for i, req, r, p, problems in rows:
        if problems or args.verbose:
            label = "%s %s" % (req.get("method", "POST"), req["path"])
            note = req.get("note")
            if note:
                label += "  (" + note + ")"
            print("%3d  %4s %5s  %-42s %s" % (
                i, r["status"] or "err", p["status"] or "err", label,
                "DIFF" if problems else "ok"))
            for problem in problems:
                print("       - " + problem)
    print("%d requests: %d match, %d differ (replay %.0f ms/request/side)"
          % (len(compared), len(compared) - failed, failed, replay_ms))
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
