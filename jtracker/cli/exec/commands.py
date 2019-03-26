import click
import json
import requests
from jtracker.execution import Executor


@click.command()
@click.option('-q', '--queue-id', help='Job queue ID')
@click.option('-k', '--parallel-workers', type=int, default=2, help='Max number of parallel workers')
@click.option('-p', '--parallel-jobs', type=int, default=1, help='Max number of parallel running jobs')
@click.option('-m', '--max-jobs', type=int, default=0, help='Max number of jobs to be run by the executor')
@click.option('-d', '--min-disk', type=int, default=0, help='Min required free disk space (in GB)')
@click.option('-b', '--job-selector', help='Execute jobs matching specified selectors, use comma to separate selectors')
@click.option('-j', '--job-file', type=click.Path(exists=True), help='Execute local job file')
@click.option('-w', '--workflow-name', help='Specify registered workflow name in format: [{owner}/]{workflow}:{ver}')
@click.option('-t', '--retries', type=click.IntRange(0, 3), default=2, help='Set retry attempts (0-3) for failed task')
@click.option('-f', '--force-restart', is_flag=True, help='Force executor restart, set previous running jobs to cancelled')
@click.option('-r', '--resume-job', is_flag=True, help='Force executor restart, set previous running jobs to resume')
@click.option('-i', '--polling-interval', type=int, default=10, help='Time interval the executor checks for new task')
@click.pass_context
def run(ctx, job_file, job_selector, queue_id, force_restart, resume_job,
             workflow_name, parallel_jobs, max_jobs, min_disk, parallel_workers, retries,
             polling_interval):
    """
    Launch JTracker executor
    """
    jt_executor = None
    try:
        jt_executor = Executor(jt_home=ctx.obj['JT_CONFIG'].get('jt_home'),
                               jt_account=ctx.obj['JT_CONFIG'].get('jt_account'),
                               ams_server=ctx.obj['JT_CONFIG'].get('ams_server'),
                               wrs_server=ctx.obj['JT_CONFIG'].get('wrs_server'),
                               jess_server=ctx.obj['JT_CONFIG'].get('jess_server'),
                               job_file=job_file,
                               job_selector=job_selector,
                               queue_id=queue_id,
                               workflow_name=workflow_name,
                               parallel_jobs=parallel_jobs,
                               max_jobs=max_jobs,
                               min_disk=min_disk * 1000000000,
                               parallel_workers=parallel_workers,
                               retries=retries,
                               force_restart=force_restart,
                               resume_job=resume_job,
                               polling_interval=polling_interval,
                               logger=ctx.obj.get('LOGGER')
                               )
    except Exception as e:
        click.echo(str(e))
        click.echo('For usage: jt exec run --help')
        ctx.abort()

    if jt_executor:
        jt_executor.run()


@click.command()
@click.option('-q', '--queue-id', help='Job queue ID', required=True)
@click.pass_context
def ls(ctx, queue_id):
    """
    Listing executors working on specified queue
    """
    jess_url = ctx.obj.get('JT_CONFIG').get('jess_server')
    owner = ctx.obj.get('JT_CONFIG').get('jt_account')

    url = "%s/executors/owner/%s/queue/%s" % (jess_url, owner, queue_id)

    r = requests.get(url)

    if r.status_code != 200:
        click.echo('List executor for: %s failed: %s' % (owner, r.text))
    else:
        try:
            rv = json.loads(r.text)
            if isinstance(rv, (list, tuple)):
                for j in rv:
                    j['queue_id'] = j.pop('job_queue_id')
                    if ctx.obj.get('JT_WRITE_OUT') == 'simple':
                        click.echo('\t'.join([
                            j['id'],
                            j['queue_id'],
                            j.get('node_id', '_null_'),
                            j.get('node_ip', '_null_'),
                            '_null_' if 'job_selector' not in j or j['job_selector'] == '' else j['job_selector']
                        ]))
                    elif ctx.obj.get('JT_WRITE_OUT') == 'json':
                        click.echo(json.dumps(j))
            else:
                click.echo(rv)
        except Exception as err:
            click.echo("Error: %s" % err)


@click.command()
@click.option('-q', '--queue-id', help='Job queue ID', required=True)
@click.option('-x', '--executor-id', help='Executor ID', required=True)
@click.option('-b', '--job-selector',
              help='Execute jobs matching specified selectors, use comma to separate selectors',
              required=True)
@click.pass_context
def selector(ctx, queue_id, executor_id, job_selector):
    """
    Set job selector for specific executor
    """
    jess_url = ctx.obj.get('JT_CONFIG').get('jess_server')
    owner = ctx.obj.get('JT_CONFIG').get('jt_account')

    url = "%s/executors/owner/%s/queue/%s/executor/%s/action" % (jess_url, owner, queue_id, executor_id)

    r = requests.put(url=url, json={'job_selector': job_selector})

    if r.status_code != 202:
        click.echo('Set job selector to executor failed: %s' % r.text)
    else:
        click.echo(r.text)
