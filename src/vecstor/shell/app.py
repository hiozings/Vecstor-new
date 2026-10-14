"""Interactive shell. It owns UI state, never database files."""
from time import perf_counter
from prompt_toolkit import PromptSession
from prompt_toolkit.history import InMemoryHistory
from prompt_toolkit.key_binding import KeyBindings
from prompt_toolkit.lexers import PygmentsLexer
from pygments.lexers.sql import SqlLexer
from rich.console import Console
from .buffer import StatementBuffer, scan
from .commands import dispatch
from .completion import SchemaCompleter
from .render import DisplayCancelled, render_result, render_stream


def standalone_command(text):
    return text.strip().startswith('.') and '\n' not in text.strip()


def clears_pending(text):
    prefix, separator, last = text.rpartition('\n')
    return bool(separator) and last.strip() == '.clear' and scan(prefix + '\n')[1] == 'normal'


def submission_ready(text):
    return standalone_command(text) or clears_pending(text) or bool(scan(text)[0])


class Shell:
    def __init__(self, client, console=None, page_size=100, continue_page=None):
        self.client = client
        self.console = console or Console()
        self.buffer = StatementBuffer()
        self.mode, self.timer, self.exiting = 'table', False, False
        self.show_plan = False
        self.page_size = max(1, page_size)
        self.continue_page = continue_page or self._continue_page
        self.completer = SchemaCompleter()

    def _continue_page(self):
        if not self.console.is_terminal:
            return True
        try:
            return PromptSession().prompt('More rows? [Enter/q] ').strip().lower() != 'q'
        except EOFError:
            return False

    def refresh_completion(self):
        try:
            self.completer.replace(self.client.describe())
        except (ValueError, OSError):
            pass  # Completion failure must not invalidate a successful command.

    def uncertain_write(self):
        if getattr(self.client, 'last_was_write', False):
            operation = getattr(self.client, 'last_operation_id', '')
            self.console.print(f'Write outcome unknown; checking operation {operation}.', markup=False)
            try:
                render_result(self.console, self.client.get_operation_status(operation), self.mode)
            except (ValueError, OSError, KeyboardInterrupt):
                self.console.print(f'Outcome unknown. Use .status {operation}; do not blindly retry.', markup=False)

    def submit(self, text):
        """Submit the complete editor contents, preserving unexecuted suffixes."""
        if clears_pending(text):
            self.buffer.reset()
            return
        if standalone_command(text):
            try:
                dispatch(self, text)
            except (ValueError, OSError, KeyboardInterrupt) as exc:
                self.console.print(f'Error: {exc or "Cancelled"}', markup=False)
                if text.strip().split()[0] == '.import':
                    self.uncertain_write()
            return
        old_pending = self.buffer.pending
        self.buffer.reset()
        try:
            statements = self.buffer.feed(text)
        except ValueError as exc:
            self.buffer.pending = old_pending
            self.console.print(f'Error: {exc}', markup=False)
            return
        for index, statement in enumerate(statements):
            started, stream = perf_counter(), None
            try:
                stream = self.client.execute_stream(statement)
                if getattr(self.client, 'last_search_mode', None) == 'approx':
                    self.console.print('Search requested mode: approx (approximate results unless exact fallback completes).', markup=False)
                completion = render_stream(self.console, stream, self.mode, self.page_size, self.continue_page, show_plan=self.show_plan)
                if self.timer:
                    self.console.print(f"Compute elapsed: {completion.get('elapsed_seconds', 0):.3f}s")
                self.refresh_completion()
            except (ValueError, OSError, KeyboardInterrupt, DisplayCancelled) as exc:
                self.buffer.pending = ''.join(statements[index + 1:]) + self.buffer.pending
                message = str(exc) if isinstance(exc, (ValueError, OSError)) else 'Cancelled; displayed result may be incomplete.'
                self.console.print(f'{message} ({index} completed; remaining input retained)', markup=False)
                if stream is not None:
                    stream.close()
                    stream = None
                self.uncertain_write()
                break
            finally:
                if stream is not None:
                    stream.close()
                if self.timer:
                    self.console.print(f'Elapsed: {perf_counter() - started:.3f}s (terminal)')


def run_shell(client):
    shell = Shell(client)
    keys = KeyBindings()

    @keys.add('enter')
    def enter(event):
        buffer = event.current_buffer
        if submission_ready(buffer.text):
            buffer.validate_and_handle()
        else:
            buffer.insert_text('\n')

    @keys.add('tab')
    def complete(event):
        buffer = event.current_buffer
        if buffer.complete_state:
            buffer.complete_next()
        else:
            buffer.start_completion(select_first=True)

    @keys.add('c-d')
    def eof(event):
        if event.current_buffer.text.strip():
            shell.console.print('Unsubmitted input retained. Use Ctrl-C or .clear before exiting.')
        else:
            event.app.exit(exception=EOFError())

    @keys.add('c-c')
    def cancel(event):
        event.app.exit(exception=KeyboardInterrupt())

    session = PromptSession(multiline=True, key_bindings=keys, history=InMemoryHistory(),
                            lexer=PygmentsLexer(SqlLexer), completer=shell.completer, complete_while_typing=False,
                            prompt_continuation=lambda width, line, wrap: '   -> ')
    shell.console.print('VecStor SQL Shell — .help for commands', markup=False)
    address = getattr(client, 'address', None)
    if address:
        shell.console.print(f'Target: {address} (connection checked on request)', markup=False)
    if not getattr(client, 'database_name', ''):
        shell.console.print('No database selected. Use .databases or .open <name>.', markup=False)
    try:
        while not shell.exiting:
            try:
                text = session.prompt(f'{getattr(client, "database_name", "") or "vecstor"}> ', default=shell.buffer.pending)
                shell.submit(text)
            except KeyboardInterrupt:
                shell.buffer.reset()
                shell.console.print('Input cleared.')
            except EOFError:
                if shell.buffer.pending.strip():
                    shell.console.print('Unsubmitted input retained. Use Ctrl-C or .clear before exiting.')
                else:
                    break
    finally:
        client.close()
    return 0
