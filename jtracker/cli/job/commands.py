import click
import json
import requests
from .utils import job_json_to_tsv


@click.command()
@click.option('-q', '--queue-id', required=True, help='Job queue ID')
@click.option('-o', '--owner', help='Queue owner account name')
@click.option('-t', '--with-task', is_flag=True, help='Report at task level')
@click.option('-s', '--status', help='Job status',type=click.Choice(
    ['running', 'queued', 'completed', 'failed', 'suspended', 'cancelled', 'submitted', 'retry', 'resume']))
@click.pass_context
def ls(ctx, queue_id, status, owner, with_task):
    """
    Listing workflow jobs in specified queue
    """
    jess_url = ctx.obj.get('JT_CONFIG').get('jess_server')
    owner = owner if owner else ctx.obj.get('JT_CONFIG').get('jt_account')

    url = "%s/jobs/owner/%s/queue/%s" % (jess_url, owner, queue_id)

    if status:
        url = url + '?state=%s' % status

    r = requests.get(url)

    if r.status_code != 200:
        click.echo('List job for: %s failed: %s' % (owner, r.text))
    else:
        try:
            rv = json.loads(r.text)
            if isinstance(rv, (list, tuple)):
                for j in rv:
                    if ctx.obj.get('JT_WRITE_OUT') == 'simple':
                        #click.echo("job_id: %s, status: %s" % (j.get('id'), j.get('state')))
                        report_list = job_json_to_tsv(j, with_task=with_task)
                        for l in report_list:
                            click.echo('\t'.join([v if isinstance(v, str) else str(v) for v in l]))
                    elif ctx.obj.get('JT_WRITE_OUT') == 'json':
                        click.echo(json.dumps(j))
            else:
                click.echo(rv)
        except Exception as err:
            click.echo("Error: %s" % err)


@click.command()
@click.option('-q', '--queue-id', required=True, help='Job queue ID')
@click.option('-j', '--job-id', required=True, help='Job ID')
@click.option('-o', '--queue-owner', help='Queue owner account name')
@click.option('-s', '--status', help='Job status',type=click.Choice(
    ['running', 'queued', 'completed', 'failed', 'suspended', 'cancelled', 'submitted', 'retry', 'resume']))
@click.pass_context
def get(ctx, queue_id, status, job_id, queue_owner):
    """
    Get workflow job in specified queue with specified job_id
    """

    jess_url = ctx.obj.get('JT_CONFIG').get('jess_server')
    queue_owner = queue_owner if queue_owner else ctx.obj.get('JT_CONFIG').get('jt_account')

    url = "%s/jobs/owner/%s/queue/%s/job/%s" % (jess_url, queue_owner, queue_id, job_id)

    if status:
        url = url + '?state=%s' % status

    r = requests.get(url)
    if r.status_code != 200:
        click.echo('Get job for: %s failed: %s' % (queue_owner, r.text))
    else:
        click.echo(r.text)


@click.command()
@click.option('-q', '--queue-id', required=True, help='Job queue ID')
@click.option('-j', '--job-id', required=True, help='Job ID')
@click.option('-o', '--queue-owner', help='Queue owner account name')
@click.pass_context
def delete(ctx, queue_id, job_id, queue_owner):
    """
    Delete workflow job that is 'queued' in specified queue with specified job_id
    """

    jess_url = ctx.obj.get('JT_CONFIG').get('jess_server')
    queue_owner = queue_owner if queue_owner else ctx.obj.get('JT_CONFIG').get('jt_account')

    url = "%s/jobs/owner/%s/queue/%s/job/%s" % (jess_url, queue_owner, queue_id, job_id)

    r = requests.delete(url)
    if r.status_code != 200:
        click.echo('Failed: %s' % r.text)
    else:
        click.echo(r.text)


@click.command()
@click.option('-q', '--queue-id', required=True, help='Job queue ID')
@click.option('-j', '--job-id', required=True, help='Job ID')
@click.option('-o', '--queue-owner', help='Queue owner account name')
@click.pass_context
def resume(ctx, queue_id, job_id, queue_owner):
    """
    Resume workflow job that is 'failed/cancelled/suspended' in specified queue with specified job_id
    """

    jess_url = ctx.obj.get('JT_CONFIG').get('jess_server')
    queue_owner = queue_owner if queue_owner else ctx.obj.get('JT_CONFIG').get('jt_account')

    url = "%s/jobs/owner/%s/queue/%s/job/%s/action" % (jess_url, queue_owner, queue_id, job_id)
    request_body = {
        'action': 'resume'
    }

    r = requests.put(url, json=request_body)
    if r.status_code != 200:
        click.echo('Failed: %s' % r.text)
    else:
        click.echo(r.text)


@click.command()
@click.option('-q', '--queue-id', required=True, help='Job queue ID')
@click.option('-j', '--job-id', required=True, help='Job ID')
@click.option('-o', '--queue-owner', help='Queue owner account name')
@click.pass_context
def reset(ctx, queue_id, job_id, queue_owner):
    """
    Reset workflow job that is 'failed/cancelled/suspended' in specified queue with specified job_id
    """

    jess_url = ctx.obj.get('JT_CONFIG').get('jess_server')
    queue_owner = queue_owner if queue_owner else ctx.obj.get('JT_CONFIG').get('jt_account')

    url = "%s/jobs/owner/%s/queue/%s/job/%s/action" % (jess_url, queue_owner, queue_id, job_id)
    request_body = {
        'action': 'reset'
    }

    r = requests.put(url, json=request_body)
    if r.status_code != 200:
        click.echo('Failed: %s' % r.text)
    else:
        click.echo(r.text)


@click.command()
@click.option('-q', '--queue-id', required=True, help='Job queue ID')
@click.option('-j', '--job-id', required=True, help='Job ID')
@click.option('-o', '--queue-owner', help='Queue owner account name')
@click.pass_context
def suspend(ctx, queue_id, job_id, queue_owner):
    """
    Suspend a queued job
    """
    # call JESS endpoint: /jobs/owner/{owner_name}/queue/{queue_id}/job/{job_id}/action
    jess_url = ctx.obj.get('JT_CONFIG').get('jess_server')
    queue_owner = queue_owner if queue_owner else ctx.obj.get('JT_CONFIG').get('jt_account')

    url = "%s/jobs/owner/%s/queue/%s/job/%s/action" % (jess_url, queue_owner, queue_id, job_id)
    request_body = {
        'action': 'suspend'
    }

    r = requests.put(url, json=request_body)
    if r.status_code != 200:
        click.echo('Failed: %s' % r.text)
    else:
        click.echo(r.text)


@click.command()
@click.option('-q', '--queue-id', required=True, help='Job queue ID')
@click.option('-j', '--job-json', required=True, help='Job JSON string or file')
@click.option('-o', '--queue-owner', help='Queue owner account name')
@click.pass_context
def add(ctx, queue_id, job_json, queue_owner):
    """
    Enqueue new job to specified queue
    """

    jess_url = ctx.obj.get('JT_CONFIG').get('jess_server')
    queue_owner = queue_owner if queue_owner else ctx.obj.get('JT_CONFIG').get('jt_account')

    url = "%s/jobs/owner/%s/queue/%s" % (jess_url, queue_owner, queue_id)

    try:  # try to decode as JSON first
        job = json.loads(job_json)
    except:
        # try treat it as file
        try:  #
            with open(job_json, 'r') as f:
                job = json.load(f)
        except:
            click.echo('"-j" must be supplied with a valid JSON string or file')
            ctx.exit()

    r = requests.post(url=url, json=job)
    if r.status_code != 200:
        click.echo('Enqueue job for: %s failed: %s' % (queue_owner, r.text))
    else:
        click.echo('Enqueue job for: %s into queue: %s succeeded' % (queue_owner, queue_id))
        click.echo(r.text)
