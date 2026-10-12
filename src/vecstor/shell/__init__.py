"""Interactive RPC-only SQL terminal."""


def run_shell(client):
    from .app import run_shell as run
    return run(client)
