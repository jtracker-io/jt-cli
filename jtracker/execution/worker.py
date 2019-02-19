import os
import re
import errno
import subprocess
import json
import requests
from time import sleep, time
from uuid import uuid4
from random import random
from .. import __version__ as ver


def download_file(local_path, url, logger):
    logger.debug('File provisioner, local_path: %s, url: %s' % (local_path, url))

    wait_time = 0
    file_sizes = []
    # Check whether file is being downloaded, if so wait.
    sleep_time = 30
    last_filesize_checks = 6
    while os.path.isfile(local_path + '.__downloading__'):
        # Need a reliable way to break out this loop if in fact no download is happening
        if os.path.isfile(local_path):
            file_sizes.append(os.path.getsize(local_path))
        else:
            file_sizes.append(0)
        if len(file_sizes) >= last_filesize_checks and \
                len(set(file_sizes[-last_filesize_checks:])) == 1:  # in the last 6 checks, file size did not change
            logger.debug('Waited %s seconds, no file size change in the last %s seconds. ' %
                         (wait_time, sleep_time * last_filesize_checks) +
                         'File seems not being downloaded. Start re-download.'
                         )
            os.remove(local_path + '.__downloading__')
            break
        sleep(sleep_time)
        wait_time += sleep_time
        logger.debug('Waited %s seconds for another worker to provision the file.' % wait_time)

    # start download if file is not ready and it is not being downloaded
    if not os.path.isfile(local_path) or \
            (not os.path.isfile(local_path + '.__ready__') and not os.path.isfile(local_path + '.__downloading__')):
        dirname = os.path.dirname(local_path)
        try:
            os.makedirs(dirname)
        except OSError as e:  # Guard against race condition
            if e.errno != errno.EEXIST:
                raise

        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        try:
            os.open(local_path + '.__downloading__', flags)  # create flag for downloading
        except OSError as e:
            raise

        if os.path.isfile(local_path + '.__ready__'):  # just in case ready flag is there
            os.remove(local_path + '.__ready__')  # remove flag

        # now actual download
        logger.debug('Downloading from: %s' % url)
        try:
            r = requests.get(url, stream=True)
            if r.status_code >= 400:
                raise Exception('Bad HTTP response code: %s' % r.status_code)

            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)
        except Exception as e:
            if os.path.isfile(local_path):
                os.remove(local_path)  # remove unfinished file
            if os.path.isfile(local_path + '.__downloading__'):  # just in case
                os.remove(local_path + '.__downloading__')  # remove flag
            raise Exception(e)

        # update the flag to indicate file is ready
        os.rename(local_path + '.__downloading__', local_path + '.__ready__')
        logger.debug('Download completed for: %s' % url)

    if os.path.isfile(local_path) and os.path.isfile(local_path + '.__ready__'):
        return True

    # it's OK to return false, file provisioning failed
    return False


class Worker(object):
    def __init__(self, jt_home=None, account_id=None, retries=2,
                 scheduler=None, node_id=None, node_ip=None, logger=None):
        self._id = str(uuid4())
        self._jt_home = jt_home
        self._account_id = account_id
        self._node_id = node_id
        self._node_ip = node_ip
        self._retries = retries
        self._scheduler = scheduler
        self._task = None
        self._logger = logger

    @property
    def id(self):
        return self._id

    @property
    def logger(self):
        return self._logger

    @property
    def jt_home(self):
        return self._jt_home

    @property
    def account_id(self):
        return self._account_id

    @property
    def executor_id(self):
        return self.scheduler.executor_id

    @property
    def queue_id(self):
        return self.scheduler.queue_id

    @property
    def workflow_id(self):
        return self.scheduler.workflow_id

    @property
    def workflow_version(self):
        return self.scheduler.workflow_version

    @property
    def retries(self):
        return self._retries

    @property
    def node_id(self):
        return self._node_id

    @property
    def node_ip(self):
        return self._node_ip

    @property
    def node_dir(self):
        return os.path.join(self.jt_home, 'account.%s' % self.account_id, 'node')

    @property
    def workflow_dir(self):
        return os.path.join(self.node_dir,
                            'workflow.%s' % self.workflow_id,
                            self.workflow_version)

    @property
    def queue_dir(self):
        return os.path.join(self.workflow_dir, 'queue.%s' % self.queue_id)

    @property
    def executor_dir(self):
        return os.path.join(self.queue_dir, 'executor.%s' % self.executor_id)

    @property
    def job_dir(self):
        return os.path.join(self.executor_dir, 'job.%s' % self.task.get('job.id'))

    @property
    def task_dir(self):
        return os.path.join(self.job_dir, 'task.%s' % self.task.get('name'))

    @property
    def scheduler(self):
        return self._scheduler

    @property
    def task(self):
        return self._task

    def next_task(self, job_state=None):
        self._task = self.scheduler.next_task(job_state=job_state)
        return self.task

    def run(self):
        # get
        if not self.task:
            raise Exception("Must first get a task before calling 'run'")

        self._init_task_dir()

        time_start = int(time())

        self.logger.info('Worker starts to work on task: %s in job: %s' % (self.task.get('name'), self.task.get('job.id')))

        file_provision_error = None
        try:
            self._stage_input_files()
        except Exception as e:
            file_provision_error = 'File provisioning error: %s' % str(e)
            self.logger.debug("File provisioning failed, error: %s" % e)

        if file_provision_error:
            success = False
            with open(os.path.join(self.task_dir, '_file_provision_err.txt'), 'a') as f:
                f.write(file_provision_error)
        else:
            command = self._task_command_builder()
            self.logger.debug("Task command is: %s" % command)

            for n in range(self.retries + 1):
                success = True  # assume task complete
                if n > 0:
                    pause = 100 * 2 ** n
                    self.logger.info('Task: %s failed, retry in %s seconds; job: %s' %
                          (self.task.get('name'), pause, self.task.get('job.id')))
                    sleep(pause)  # pause before retrying
                    self.logger.info('No %s retry on task: %s; job: %s' %
                          (n, self.task.get('name'), self.task.get('job.id')))
                try:
                    p = subprocess.Popen([command], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                    stdout, stderr = p.communicate()
                except Exception as e:
                    success = False

                # log the stdout/stderr
                with open(os.path.join(self.task_dir, 'stdout.txt'), 'a') as o:
                    o.write("Run no: %s, STDOUT at: %s\n" % (n + 1, int(time())))
                    o.write(stdout.decode("utf-8"))
                with open(os.path.join(self.task_dir, 'stderr.txt'), 'a') as e:
                    e.write("Run no: %s, STDERR at: %s\n" % (n + 1, int(time())))
                    e.write(stderr.decode("utf-8"))

                if p.returncode != 0 or success is False:
                    if 'KeyboardInterrupt' in stderr.decode("utf-8"):
                        success = None  # task cancelled
                        break
                    else:
                        success = False  # task failed
                else:
                    break  # success

        time_end = int(time())

        """
        TODO: will need to capture and search for results, and prepare output json which was previously
              done by the tool, now we have to do it within JTracker
        """

        # get output.json
        try:
            with open(os.path.join(self.task_dir, 'output.json'), 'r') as f:
                output = json.load(f)
        except:
            output = dict()  # when there is no output.json file

        _jt_ = {
            'jtcli_version': ver,
            'worker_id': self.id,
            'executor_id': self.executor_id,
            'workflow_id': self.workflow_id,
            'queue_id': self.queue_id,
            'node_id': self.node_id,
            'node_ip': self.node_ip,
            'task_dir': self.task_dir,
            'state': 'completed' if success else 'failed' if success is False else 'cancelled',
            'wall_time': {
                'start': time_start,
                'end': time_end
            }
        }

        output.update({'_jt_': _jt_})

        job_id = self.task.get('job.id')
        task_name = self.task.get('name')
        if success:
            self.logger.info('Task completed, task: %s, job: %s' % (task_name, job_id))
            self.scheduler.task_completed(job_id=job_id,
                                          task_name=task_name,
                                          output=output)
            exit(0)
        elif success is None:
            self.logger.info('Task cancelled, task: %s, job: %s' % (task_name, job_id))
            exit(2)
        else:
            self.logger.info('Task failed, task: %s, job: %s' % (task_name, job_id))
            self.logger.info('STDERR: %s' % file_provision_error if file_provision_error else stderr.decode("utf-8"))
            self.scheduler.task_failed(job_id=job_id,
                                       task_name=task_name,
                                       output=output)
            exit(1)

    def _init_task_dir(self):
        try:
            os.makedirs(self.task_dir)
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

        os.chdir(self.task_dir)

    def _task_command_builder(self):
        """
        Take task dictionary as input build full command in string (to be executed by a JT worker)
        :param task: (dict)
        task_dict = {
                    'task': task_name,
                    'input': {},
                    'command': "--file ${file} --words ${sep=',' words}",
                    'runtime': {}
                }
        :return: full command (string)

        flatten list of values using 'sep' or 'pref', eg, ${sep=',' words} or ${prefix='-w' words}
        """
        task = json.loads(self.task.get('task_file'))

        if not isinstance(task, dict):
            raise ValueError('Must provide task as dictionary type.')

        p = re.compile("\$\{([a-zA-Z]+[a-zA-Z0-9_.]*|sep='([,.\-\s\w]+)'\s+([a-zA-Z]+[a-zA-Z0-9_.]*)?)\}", re.MULTILINE)

        command_str = task.get('command')
        input_dict = task.get('input', {})

        self.logger.debug("Task raw command is: %s" % command_str)
        self.logger.debug("Task dict is: %s" % input_dict)

        # parse out all variables and replace with values from input
        replaced = False
        for m in p.findall(command_str):
            if m[0].startswith('sep='):
                sep = m[1]
                input_var = input_dict.get(m[2]) if isinstance(input_dict.get(m[2]), list) else [input_dict.get(m[2], '')]
                value = sep.join(input_var)
            else:
                value = input_dict.get(m[0], '')

            replaced = True
            command_str = command_str.replace("${%s}" % m[0], value, 1)

        if not replaced:  # backward compatibility, if no argument then add the whole task_file string as argument
            command_str = "%s \"%s\"" % (command_str, json.dumps(task).replace('"', '\\"') if task else '')

        return "PATH=%s:$PATH %s" % (os.path.join(self.workflow_dir, 'workflow', 'tools'), command_str)

    def _stage_input_files(self):
        task_file_json = json.loads(self.task.get('task_file'))
        input_ = task_file_json.get('input', {})

        self.logger.debug("Before file provisioning, input: %s" % input_)

        for k in input_:
            if isinstance(input_[k], str):
                local_path = self._provision_file(input_[k])
                if local_path: input_[k] = local_path
            elif isinstance(input_[k], list):
                for i in range(len(input_[k])):
                    if not isinstance(input_[k][i], str):
                        continue
                    local_path = self._provision_file(input_[k][i])
                    if local_path: input_[k][i] = local_path

        self.logger.debug("After file provisioning, input: %s" % input_)
        task_file_json['input'] = input_
        self._task['task_file'] = json.dumps(task_file_json)

    def _provision_file(self, file_url):
        m = re.match("\[(.+)\]((http|https)://.+)", file_url)
        local_path, url = None, None

        if m:
            local_path, url = m.group(1), m.group(2)
            if '${_wf_data}' in local_path:
                local_path = local_path.replace('${_wf_data}', os.path.join(self.workflow_dir, 'data'))
            else:
                local_path = os.path.join(self.job_dir, 'data', local_path)  # job level data

        # comment this out, as we prefer only provision the file when local_path is provided
        # elif file_url.startswith('http://') or file_url.startswith('https://'):
        #    # TODO: need to take care when basename contains special characters,
        #    #       eg, 'https://example.com/download?file=abc.txt' basename will be 'download?file=abc.txt'
        #    #       do we want to hash the basename or maybe the whole url as local name?
        #    local_path, url = os.path.join(self.task_dir, os.path.basename(file_url)), file_url

        elif file_url.startswith('file://'):
            local_path, url = file_url.replace('file://', '', 1), None
            if not local_path.startswith('/'):
                local_path = os.path.join(self.task_dir, local_path)

        if url:  # perform the actual file previsioning
            if not download_file(local_path, url, self.logger):
                raise('File provisioning failed, url: %s' % url)

        return local_path
