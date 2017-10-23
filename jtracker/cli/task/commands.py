import click


@click.command()
@click.pass_context
def list(ctx):
    """
    Listing workflow tasks
    """
    click.echo('task list subcommand')
    click.echo(ctx.obj)


class TaskClient(object):
    pass
