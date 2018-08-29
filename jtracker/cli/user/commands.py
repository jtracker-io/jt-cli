import click
import requests
import json
from jtracker.exceptions import AMSNotAvailable, AccountNameNotFound


@click.command()
@click.pass_context
def ls(ctx):
    """
    Listing users
    """
    click.echo('user list subcommand not implemented yet')


@click.command()
@click.option('-u', '--user', required=True, help='User name')
# @click.option('-e', '--email', required=True, help='User email')
@click.pass_context
def signup(ctx, user):
    """
    Sign up as a new user
    """
    ams_url = ctx.obj.get('JT_CONFIG').get('ams_server')

    url = "%s/accounts" % ams_url

    r = requests.post(url=url, json={
        "account_type": "user",
        "name": user
    })

    if r.status_code != 200:
        click.echo('Account sign up failed: %s' % r.text)
    else:
        rv = json.loads(r.text)
        click.echo("Account sign up succeeded")
        click.echo("user_name: %s\nuser_id: %s" %(rv.get('name'), rv.get('id')))


@click.command()
@click.option('-u', '--user', required=True, help='User name')
@click.pass_context
def login(ctx, user):
    """
    User log in
    """
    # first make sure user can log in
    # for now as long as account exists, it's valid to switch to it

    ams_url = ctx.obj.get('JT_CONFIG').get('ams_server')

    url = "%s/accounts/%s" % (ams_url, user)
    r = requests.get(url=url)
    if r.status_code != 200:
        click.echo('Log in failed for: %s' % user)

    else:
        jtconfig_file = ctx.obj.get('JT_CONFIG_FILE')
        with open(jtconfig_file, 'r') as f:
            lines = f.readlines()

        with open(jtconfig_file, 'w') as f:
            for l in lines:
                if l.startswith('jt_account:'):
                    l = 'jt_account: %s\n' % user
                f.write(l)

        # TODO: write the logged user name in the config file
        click.echo('Logged in as: %s' % user)


@click.command()
@click.pass_context
def whoami(ctx):
    """
    Get the current user
    """
    # need to check whether user already logged in
    current_acc = ctx.obj.get('JT_CONFIG').get('jt_account')
    click.echo("Current account in use: %s" % current_acc)


@click.command()
@click.pass_context
def delete(ctx):
    """
    Delete a user
    """
    click.echo('user delete subcommand not implemented yet')


@click.command()
@click.pass_context
def update(ctx):
    """
    Update user info
    """
    click.echo('user update subcommand not implemented yet')
