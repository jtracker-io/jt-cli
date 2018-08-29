import os
import errno
import subprocess
import json
from time import sleep, time
from uuid import uuid4
from random import random
from .. import __version__ as ver


class Worker(object):
    def __init__(self, jt_home=None, account_id=None, scheduler=None, node_id=None, logger=None):
        self._id = str(uuid4())
        self._jt_home = jt_home
        self._account_id = account_id
        self._node_id = node_id
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
    def node_id(self):
        return self._node_id

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

    def run(self, retry=2):
        # get
        if not self.task:
            raise Exception("Must first get a task before calling 'run'")

        self._init_task_dir()

        time_start = int(time())

        self.logger.info('Worker starts to work on task: %s in job: %s' % (self.task.get('name'), self.task.get('job.id')))

        cmd = "PATH=%s:$PATH %s" % (os.path.join(self.workflow_dir, 'workflow', 'tools'),
                                    json.loads(self.task.get('task_file')).get('command'))

        arg = "%s" % self.task.get('task_file').replace('"', '\\"') if self.task else ''

        for n in range(retry + 1):
            success = True  # assume task complete
            if n > 0:
                pause = 100 * 2 ** n
                self.logger.info('Task: %s failed, retry in %s seconds; job: %s' %
                      (self.task.get('name'), pause, self.task.get('job.id')))
                sleep(pause)  # pause before retrying
                self.logger.info('No %s retry on task: %s; job: %s' %
                      (n, self.task.get('name'), self.task.get('job.id')))
            try:
                p = subprocess.Popen(["%s \"%s\"" % (cmd, arg)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
                stdout, stderr = p.communicate()
            except Exception as e:
                success = False

            if p.returncode != 0 or success is False:
                with open(os.path.join(self.task_dir, 'stdout.txt'), 'a') as o:
                    o.write("Run no: %s, STDOUT at: %s\n" % (n+1, int(time())))
                    o.write(stdout.decode("utf-8"))
                with open(os.path.join(self.task_dir, 'stderr.txt'), 'a') as e:
                    e.write("Run no: %s, STDERR at: %s\n" % (n+1, int(time())))
                    e.write(stderr.decode("utf-8"))
                if 'KeyboardInterrupt' in stderr.decode("utf-8"):
                    success = None  # task cancelled
                    break
                else:
                    success = False  # task failed
            else:
                break  # success

        time_end = int(time())

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
            self.logger.info('STDERR: %s' % stderr.decode("utf-8") )
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
