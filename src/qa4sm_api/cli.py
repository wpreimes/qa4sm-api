import click
from qa4sm_api.client_api import Connection

import click
import os
import json
from pathlib import Path

CONFIG_FILE = Path.home() / ".qa4sm" / "config.json"

def setup_token(instance, token: str):
    CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_FILE.write_text(json.dumps({"token": token}))

def load_token() -> str | None:
    if CONFIG_FILE.exists():
        return json.loads(CONFIG_FILE.read_text()).get("token")
    return None

def require_auth(f):
    """Decorator that injects the token or exits with an error."""
    @click.pass_context
    def wrapper(ctx, *args, **kwargs):
        token = load_token()
        if not token:
            raise click.ClickException("Not logged in. Run: qa4sm login <token>")
        return ctx.invoke(f, *args, token=token, **kwargs)
    return wrapper


@click.group()
def cli():
    """qa4sm — command line interface."""
    pass

@cli.command()
@click.argument("token")
def login(token):
    """Authenticate with a personal access token."""
    # Optionally validate the token against the API here
    save_token(token)
    click.echo("Logged in successfully.")


if __name__ == "__main__":
    cli()