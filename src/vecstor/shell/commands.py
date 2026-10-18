"""Local shell commands; all database access goes through SessionClient."""
import shlex
from .render import render_result

HELP = '''SQL: single-table SELECT, aggregates, CREATE/DROP TABLE, INSERT VALUES,
UPDATE, DELETE; vector_distance(vector, [0.1, 0.2]) ORDER BY with LIMIT.
JOIN, CTE, file functions and explicit transactions are unsupported.
Vector SELECT may end with SETTINGS device='gpu',mode='approx',path='search-first'.
End SQL with ; then Enter. Multiple statements commit independently.
.help                     Show this help
.databases                List managed databases
.open NAME [--create]     Select or explicitly create a database
.datasets                 List managed import sources
.import SOURCE TABLE      Import managed NPZ into a new table
.tables / .schema [TABLE] Show tables or columns
.status OPERATION_ID      Confirm an uncertain write
.mode table|json          Select output format
.timer on|off             Toggle elapsed time
.search [reset|KEY VALUE] Show/reset search controls; mode exact|approx,
                         path filter-first|search-first, nprobe, candidates,
                         max-candidates, max-rounds, fallback exact|error
.search profiles          List calibrated profiles for this database
.search profile ID|off    Select/disable a snapshot-bound quality profile
.index build TABLE [--nlist N] [--timeout S] [--device cpu|gpu] [--gpu-id 0]
                         Explicitly build CPU index / GPU replica
.index list|clear [TABLE] Inspect/clear compute indexes
.index publish TABLE [--nlist N] [--operation-id ID] [--timeout S]
.index load TABLE [--device cpu|gpu] [--artifact-id ID] [--timeout S]
.index inspect TABLE     Compare published artifact and local readiness
.index published [TABLE] [--limit N] [--cursor ID]
.index publication-status OPERATION_ID
.index prune [--keep-last N] [--execute --operation-id ID]
                         Preview by default; clear never deletes stored artifacts
.plan on|off              Toggle detailed execution plans
EXPLAIN [ANALYZE] SELECT  Explain a vector SELECT
.clear / .quit            Clear input / exit
Ctrl-C clears input or cancels execution; Ctrl-D exits only empty input.
Example: .open demo --create
         SELECT id, category FROM items LIMIT 10;'''


def dispatch(shell, text):
    parts = shlex.split(text)
    command, args = parts[0], parts[1:]
    arities = {'.help': (0,), '.databases': (0,), '.open': (1, 2), '.datasets': (0,), '.import': (2,), '.tables': (0,), '.schema': (0, 1), '.status': (1,), '.mode': (1,), '.timer': (1,), '.clear': (0,), '.quit': (0,), '.search': (0, 1, 2), '.index': (1, 2, 3, 4, 6, 8, 10), '.plan': (1,)}
    if command not in arities or (command!='.index' and len(args) not in arities[command]):
        raise ValueError('Unknown command or invalid arguments. Use .help.')
    client = shell.client
    if command == '.search':
        if not args:
            result = client.search_options.to_dict()
        elif args == ['profiles']:
            result = client.list_quality_profiles()
        elif args == ['reset']:
            result = client.reset_search()
        elif len(args) == 2:
            result = client.set_search(*args)
        else:
            raise ValueError('Usage: .search [reset|KEY VALUE]')
        render_result(shell.console, result, shell.mode)
    elif command == '.plan':
        if args[0] not in ('on', 'off'):
            raise ValueError('Usage: .plan on|off')
        shell.show_plan = args[0] == 'on'
    elif command == '.index':
        from .index_commands import execute_index
        result=execute_index(client,args)
        render_result(shell.console, result, shell.mode)
    elif command == '.help':
        shell.console.print(HELP, markup=False)
    elif command == '.quit':
        shell.exiting = True
    elif command == '.clear':
        shell.buffer.reset()
    elif command == '.mode':
        if args[0] not in ('table', 'json'):
            raise ValueError('Usage: .mode table|json')
        shell.mode = args[0]
    elif command == '.timer':
        if args[0] not in ('on', 'off'):
            raise ValueError('Usage: .timer on|off')
        shell.timer = args[0] == 'on'
    else:
        if command == '.open':
            if len(args) == 2 and args[1] != '--create':
                raise ValueError('Usage: .open NAME [--create]')
            result = client.open_database(args[0], create=len(args) == 2)
            shell.refresh_completion()
        elif command == '.databases':
            result = client.list_databases()
        elif command == '.datasets':
            result = client.list_datasets()
        elif command == '.import':
            result = client.import_dataset(*args)
            shell.refresh_completion()
        elif command == '.tables':
            result = client.execute('SHOW TABLES;')
            shell.completer.remember(result)
        elif command == '.schema':
            result = client.describe(args[0] if args else None)
            shell.completer.remember(result)
        else:
            result = client.get_operation_status(args[0])
        render_result(shell.console, result, shell.mode)
