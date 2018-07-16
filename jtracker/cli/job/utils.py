import json
import datetime


def job_json_to_tsv(job_json, with_task=False):
    ret = []
    # convert job json to list of fields
    # TSV fields: job_id, job_name, job_state, task_name, task_state, task_run_num, task_end_at, task_len executor_id, node_id
    job_id = job_json.get('id')
    job_name = job_json.get('name')
    job_state = job_json.get('state')
    if with_task:
        for t in job_json.get('tasks'):
            task_name = t
            task = job_json.get('tasks').get(t)
            task_state = task.get('state')
            task_file = json.loads(task.get('task_file'))
            executor_id = '_null_'
            node_id = '_null_'
            task_run_num = '_null_'
            task_end_at = '_null_'
            task_len = '_null_'
            task_runs = task_file.get('output', [])
            if task_runs:
                task_run_num = len(task_runs)
                executor_id = task_runs[-1]['_jt_']['executor_id']
                node_id = task_runs[-1]['_jt_']['node_id']
                seconds_since_epoch = task_runs[-1]['_jt_']['wall_time']['end']
                task_end_at = datetime.datetime.utcfromtimestamp(seconds_since_epoch).isoformat()
                task_len = task_runs[-1]['_jt_']['wall_time']['end'] - task_runs[-1]['_jt_']['wall_time']['start']
            ret.append([job_id, job_name, job_state, task_name, task_state,
                        task_run_num, task_end_at, task_len, executor_id, node_id])

    else:
        ret.append([job_id, job_name, job_state])

    return ret
