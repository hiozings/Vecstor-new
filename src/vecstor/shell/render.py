"""Bounded rendering of result batches, with explicit continuation."""
import json
from rich.table import Table


class DisplayCancelled(Exception):
    pass


def render_rows(console, columns, rows, mode='table'):
    if mode == 'json':
        console.print(json.dumps({'columns': columns, 'rows': rows}, ensure_ascii=False), markup=False, highlight=False, soft_wrap=True)
        return
    table = Table(show_lines=False)
    for column in columns:
        table.add_column(str(column['name'] if isinstance(column, dict) else column))
    for row in rows:
        table.add_row(*(str(v) if v is not None else 'NULL' for v in row))
    console.print(table)


def render_result(console, result, mode='table'):
    if isinstance(result, list):
        if result:
            columns = list(result[0])
            render_rows(console, columns, [[row.get(k) for k in columns] for row in result], mode)
        else:
            console.print('(empty)')
    elif 'rows' in result:
        render_rows(console, result.get('columns', []), result['rows'], mode)
    else:
        console.print(json.dumps(result, ensure_ascii=False), markup=False, highlight=False)


def render_stream(console, stream, mode, page_size, continue_page, show_plan=False):
    columns, page, total, complete = [], [], 0, None
    for event in stream:
        kind = event['kind']
        if kind == 'schema':
            columns = event['columns']
        elif kind == 'rows':
            for row in event['rows']:
                if len(page) == page_size:
                    render_rows(console, columns, page, mode)
                    page = []
                    if not continue_page():
                        raise DisplayCancelled()
                page.append(row)
                total += 1
        elif kind == 'complete':
            complete = event
    if complete is None:
        raise ValueError('Result stream ended without completion; displayed rows may be incomplete.')
    if page or columns and not total:
        render_rows(console, columns, page, mode)
    affected = complete.get('affected_rows')
    console.print(f'{affected} affected' if affected is not None else f'{total} rows')
    plan = complete.get('execution_plan')
    if plan is not None:
        if mode == 'json':
            console.print(json.dumps({'execution_plan': plan}, ensure_ascii=False), markup=False, highlight=False, soft_wrap=True)
        else:
            actual = plan.get('actual_mode', plan.get('effective_mode', 'unknown'))
            console.print(f"Search result mode: {actual}; fallback: {plan.get('fallback_reason') or 'none'}", markup=False)
            if show_plan:
                console.print(json.dumps(plan, ensure_ascii=False, indent=2), markup=False, highlight=False)
    return complete
