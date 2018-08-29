import click


@click.command()
@click.pass_context
def ls(ctx):
    """
    Listing organizations
    """
    click.echo('org list subcommand not implemented yet')

