#!/usr/bin/env python3
"""Generate a static HTML report beside a Jepsen acceptance report."""
import argparse
from html import escape
from pathlib import Path
import re
from urllib.parse import quote, unquote, urlsplit


def page(title, body):
    return (
        '<!doctype html><html lang="en"><meta charset="utf-8">'
        f'<title>{escape(title)}</title>'
        '<style>body{font:16px system-ui;max-width:75rem;margin:2rem auto;padding:0 1rem}'
        'td,th{padding:.5rem;text-align:left}pre{white-space:pre-wrap}'
        'img{max-width:100%;height:auto}</style>'
        f'<h1>{escape(title)}</h1>{body}</html>'
    )


def link(path, label):
    return f'<a href="{escape(quote(path))}">{escape(label)}</a>'


def write_details(run, name, status):
    details = run / "static-report.html"
    results = run / "results.edn"
    body = "<ul>" + "".join(
        f"<li>{link(file.name, file.name)}</li>"
        for file in sorted(run.iterdir())
        if file.is_file() and file.name != details.name
    ) + "</ul>"
    if results.is_file():
        body += f"<h2>Checker result</h2><pre>{escape(results.read_text(encoding='utf-8'))}</pre>"
    for graph in sorted(run.glob("*.png")):
        body += f'<h2>{escape(graph.stem)}</h2><img src="{quote(graph.name)}" alt="{escape(graph.stem)}" loading="lazy">'
    details.write_text(page(name, f"<p>{escape(status)}</p>" + body), encoding="utf-8")
    return details


def markdown_results(report):
    for line in report.read_text(encoding="utf-8").splitlines():
        match = re.fullmatch(r"\| (.+?) \| (.+?) \| (.+?) \|", line)
        if not match or match[1] == "Test":
            continue
        name, status, reference = match.groups()
        target = re.fullmatch(r"\[report\]\((.+)\)", reference)
        run = None
        if target:
            url_path = unquote(urlsplit(target[1]).path)
            if url_path.startswith("/files/"):
                run = report.parent / "store" / url_path[len("/files/"):]
            elif url_path.startswith("store/") and url_path.endswith("/static-report.html"):
                run = (report.parent / url_path).parent
        yield name, status, run


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("report", nargs="?", type=Path, help="legacy jepsen-report-*.md")
    parser.add_argument("--output", type=Path, help="path to the combined HTML report")
    parser.add_argument("--result", action="append", nargs=3, metavar=("NAME", "EXIT_CODE", "RUN_DIR"),
                        help="scenario name, exit code, and run directory (empty if unavailable)")
    args = parser.parse_args()
    if args.report and not args.result and not args.output:
        output = args.report.resolve().with_suffix(".html")
        results = markdown_results(args.report.resolve())
    elif args.output and args.result and not args.report:
        output = args.output.resolve()
        results = []
        for name, exit_code, run_dir in args.result:
            if not exit_code.isdecimal():
                parser.error(f"invalid exit code: {exit_code}")
            code = int(exit_code)
            status = "success" if code == 0 else f"FAILED (exit {code})"
            results.append((name, status, Path(run_dir) if run_dir else None))
    else:
        parser.error("pass a Markdown report or use --output with one or more --result options")

    root = output.parent
    store = (root / "store").resolve()
    rows = []
    for name, status, run in results:
        scenario = escape(name)
        if run is not None:
            run = run.resolve()
            if store in run.parents and root in run.parents and run.is_dir():
                details = write_details(run, name, status)
                scenario = link(details.relative_to(root).as_posix(), name)
        rows.append(f"<tr><td>{scenario}</td><td>{escape(status)}</td></tr>")
    if not rows:
        parser.error("no scenarios found in the acceptance report")
    table = "<table><tr><th>Scenario</th><th>Status</th></tr>" + "".join(rows) + "</table>"
    output.write_text(page("Jepsen acceptance report", table), encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
