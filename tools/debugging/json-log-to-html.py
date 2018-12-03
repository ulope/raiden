"""
Utility to format Raiden json logs into HTML.
Colorizes log key-values according to their hash to make debugging easier.
Allows to filter records by `event`.
When processing multiple files make sure to set PYTHONHASHSEED to a fixed value,
otherwise the colors of values will not match up.
"""

import hashlib
import json
from collections import Counter, namedtuple
from datetime import datetime
from html import escape
from json import JSONDecodeError

import click

try:
    from colour import Color
except ImportError:
    click.secho('This tool requires the `colour` package.')
    import sys
    sys.exit(1)


Line = namedtuple('Line', ('event', 'timestamp', 'logger', 'level', 'fields'))

TEMPLATE = """\
<!doctype html>
<html>
<head>
<style>
* {{
    font-family: Helvetica, sans-serif
}}
body {{
    background: #202020;
    color: white;
}}
table {{
    white-space: nowrap;
}}
table tr.head {{
    position: sticky;
}}
table tr:nth-child(2) {{
    padding-top: 15px;
}}
table tr:nth-child(odd) {{
    background-color: #303030
}}
table td {{
    padding-right: 5px;
}}
.lvl-debug {{
    color: #20d0d0;
}}
.lvl-info {{
    color: #20d020;
}}
.lvl-warning {{
    color: #d0d020;
}}
.lvl-error {{
    color: #d04020;
}}
.fn {{
    color: #f040f0;
}}
</style>
<body>
<h1>{name}</h1>
<h2>Generated on: {date:%Y-%m-%d %H:%M}</h2>
<table>
{table_header}
{table_rows}
</table>
</body>
</html>
"""


def rgb_color_picker(obj, min_luminance=None, max_luminance=None):
    """Modified version of colour.RGB_color_picker"""
    color_value = int.from_bytes(
        hashlib.md5(str(obj).encode('utf-8')).digest(),
        'little',
    ) % 0xffffff
    color = Color(f'#{color_value:06x}')
    if min_luminance and color.get_luminance() < min_luminance:
        color.set_luminance(min_luminance)
    elif max_luminance and color.get_luminance() > max_luminance:
        color.set_luminance(max_luminance)
    return color


def parse_log(log_file):
    known_fields = Counter()
    log_records = []
    for i, line in enumerate(log_file, start=1):
        try:
            line_dict = json.loads(line.strip())
        except JSONDecodeError as ex:
            click.secho(f'Error parsing line {i}: {ex}')
            continue
        log_records.append(
            Line(
                line_dict.pop('event'),
                line_dict.pop('timestamp'),
                line_dict.pop('logger'),
                line_dict.pop('level'),
                line_dict,
            ),
        )
        for field_name in line_dict.keys():
            known_fields[field_name] += 1

    return log_records, known_fields


def filter_records(log_records, drop_event):
    return (
        record for record in log_records
        if record.event.lower() not in drop_event
    )


def render(name, log_records, known_fields):
    sorted_known_fields = [name for name, count in known_fields.most_common()]
    header = (
        "<tr class=\"head\">"
        "<td>Event</td>"
        "<td>Timestamp</td>"
        "<td>Logger</td>"
        "<td>Level</td>"
        "<td>Fields</td>"
        "</tr>"
    )
    rows = []
    for record in log_records:
        event_color = rgb_color_picker(record.event, min_luminance=0.6)
        row = [
            f"<tr class=\"lvl-{record.level}\">"
            f"<td style=\"color: {event_color}\"><b>{record.event}</b></td>"
            f"<td>{record.timestamp}</td>"
            f"<td>{record.logger}</td>"
            f"<td>{record.level}</td>"
            "<td>",
        ]
        for field_name in sorted_known_fields:
            if field_name not in record.fields:
                continue
            field_value = record.fields[field_name]
            colorized_value = str(colorize_value(field_value, min_luminance=0.6))
            row.append(
                f"<span class=\"fn\">{field_name}</span>"
                f"="
                f"{colorized_value} ",
            )
        row.append("</td></tr>")
        rows.append("".join(row))
    return TEMPLATE.format(
        name=name,
        date=datetime.now(),
        table_header=header,
        table_rows="\n".join(rows),
    )


def colorize_value(value, min_luminance):
    if isinstance(value, (list, tuple)):
        return type(value)(colorize_value(inner, min_luminance) for inner in value)
    elif isinstance(value, dict):
        return {
            colorize_value(k, min_luminance): colorize_value(v, min_luminance)
            for k, v in value.items()
        }
    str_value = str(value)
    color = rgb_color_picker(str_value, min_luminance=min_luminance)
    return f'<span style="color: {color.web}">{escape(str_value)}</span>'


@click.command(help=__doc__)
@click.argument('log-file', type=click.File('rt'))
@click.option('-o', '--output', type=click.File('wt'), default='-')
@click.option(
    '-d',
    '--drop-event',
    multiple=True,
    help=(
        'Filter out log records with the given event. '
        'Case insensitive. Can be given multiple times.'
    ),
)
def main(log_file, drop_event, output):
    log_records, known_fields = parse_log(log_file)
    log_records = filter_records(log_records, set(d.lower() for d in drop_event))
    print(render(log_file.name, log_records, known_fields), file=output)


if __name__ == "__main__":
    main()
