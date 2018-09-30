import click
import yaml


@click.command()
@click.pass_context
def show(ctx):
    """
    Show current configuration
    """
    click.echo(yaml.dump(ctx.obj.get('JT_CONFIG'), default_flow_style=False))


@click.command()
@click.pass_context
def update(ctx):
    """
    Update JTracker CLI configuration
    """
    click.echo('*** to be implemented ***')

