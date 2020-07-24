'''
Copyright (c) 2019 HLRS. All rights reserved.

This file is part of Croupier.

Croupier is free software: you can redistribute it and/or modify it
under the terms of the Apache License, Version 2.0 (the License) License.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT ANY WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT, IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT
OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

See README file for full disclaimer information and LICENSE file for full
license information in the project root.

@author: Dineshkumar RAJAGOPAL
         High-Performance Computing Center. Stuttgart
         e-mail: hpcdraja@hlrs.de

spark.py
'''
import json
# from time import gmtime, strftime
from inspect import currentframe, getframeinfo
from paramiko import AuthenticationException
from croupier_plugin.ssh import SshClient
from croupier_plugin.workload_managers.workload_manager import (
    WorkloadManager,
    get_prevailing_state)


class Spark(WorkloadManager):
    """ Spark Workload Manger Driver """

    def _build_container_script(self, name, job_settings, logger):
        frameinfo = getframeinfo(currentframe())
        logger.debug("{2}: {0} - {1}".format(frameinfo.filename,
                                             frameinfo.lineno,
                                             frameinfo.function))
        ret_str = "Script not applicable for Spark!!! exception " +     \
            "at {0} - {2} - {1}"
        logger.error(ret_str.format(frameinfo.filename, frameinfo.lineno,
                                    frameinfo.function))
        return None

    def _build_job_submission_call(self, name, job_settings, logger):
        # check input information correctness
        frameinfo = getframeinfo(currentframe())
        logger.debug("{2}: {0} - {1}".format(frameinfo.filename,
                                             frameinfo.lineno,
                                             frameinfo.function))
        if not isinstance(job_settings, dict) or \
                not isinstance(name, basestring):
            return {'error': "Incorrect inputs"}

        if 'type' not in job_settings or 'application' not in job_settings:
            return {'error': "'type' and 'application' " +
                    "must be defined in job settings"}

        if (job_settings['type'] != 'SPARK'):
            return {'error': "error in 'type' value"}

        if 'pre' in job_settings:
            for entry in job_settings['pre']:
                spark_call += entry + '; '

        # Build single line command to submit jobs through spark_submit
        spark_call = "nohup sh spark-submit --name " + str(name) + " "
        spark_call += self._parse_spark_job_settings(name,
                                                     job_settings,
                                                     None, None, logger)
        spark_call += " &"
        response = {'call': spark_call}
        logger.debug("{0}: response cmd: {1}".format(frameinfo.function, response))
        return response

    def _build_job_cancellation_call(self, name, ssh_client, logger):
        frameinfo = getframeinfo(currentframe())
        logger.debug("{2}: {0} - {1}".format(frameinfo.filename,
                                             frameinfo.lineno,
                                             frameinfo.function))
        call = "curl http://{0}:`cat /security/secrets/{0}.mesos" +         \
            "`@localhost:5050/frameworks"

        user = ssh_client._user
        spark_cancel_call = None
        call_format = call.format(user)

        output, exit_code = ssh_client.execute_shell_command(call_format,
                                                             wait_result=True)

        json_output = json.loads(output)
        running_frameworks = json_output["frameworks"]
        if exit_code == 0:
            spark_cancel_call = self._parse_running_frameworks(
                running_frameworks, user, name, logger)
        else:
            logger.warning("spark_cancel_call failed : {0}".format(
                spark_cancel_call))

        # frameworkId = job_settings['frameworkId'] , Save fameworkID and
        # cancel specific jobs
        logger.debug("spark_cancel_call: {0}".format(spark_cancel_call))
        return spark_cancel_call

    def _parse_running_frameworks(self, running_frameworks, user, name,
                                  logger):
        frameinfo = getframeinfo(currentframe())
        logger.debug("{2}: {0} - {1}".format(frameinfo.filename,
                                             frameinfo.lineno,
                                             frameinfo.function))
        spark_cancel_call = None
        spark_cancel_fmt = "curl -X POST http://{1}:`cat /security/" +     \
            "secrets/{1}.mesos`@localhost:5050/teardown -d 'frameworkId={0}'; "
        for run_framework in running_frameworks:
            if run_framework['name'] == name:
                if spark_cancel_call is None:
                    spark_cancel_call = spark_cancel_fmt.format(
                        run_framework["id"], user)
                else:
                    spark_cancel_call = spark_cancel_call +                 \
                        spark_cancel_fmt.format(run_framework["id"], user)

        logger.debug("{0}: cancel:{1}, user:{2}".format(frameinfo.function,
                                                        spark_cancel_call,
                                                        user))
        return spark_cancel_call

    def _parse_spark_job_settings(self, job_id, job_settings, prefix, suffix,
                                  logger):
        frameinfo = getframeinfo(currentframe())
        logger.debug("{2}: {0} - {1}".format(frameinfo.filename,
                                             frameinfo.lineno,
                                             frameinfo.function))
        _prefix = prefix if prefix else ''
        _suffix = suffix if suffix else ''
        _settings = ''

        # Check if exists and has content
        def check_job_settings_key(job_settings, key):
            return key in job_settings and str(job_settings[key]).strip()

        # Spark settings
        if check_job_settings_key(job_settings, 'class_name'):
            _settings += _prefix + ' --class ' + \
                str(job_settings['class_name']) + _suffix
        else:
            logger.warning("pls specify class name in the blueprint")

        if check_job_settings_key(job_settings, 'total_executor_cores'):
            _settings += _prefix + ' --total-executor-cores ' + \
                str(job_settings['total_executor_cores']) + _suffix
        else:
            _settings += _prefix + ' --total-executor-cores 1' + _suffix
            logger.warning("executor cores detail is necessaery in blueprint")

        if check_job_settings_key(job_settings, 'executor_memory'):
            _settings += _prefix + ' --executor-memory ' + \
                str(job_settings['executor_memory']) + _suffix
        else:
            _settings += _prefix + ' --executor-memory 2G ' + _suffix
            logger.warning("executor memory detail is necessary in blueprint")

        if check_job_settings_key(job_settings, 'driver_cores'):
            _settings += _prefix + ' --driver-cores ' + \
                str(job_settings['driver_cores']) + _suffix
        else:
            _settings += _prefix + ' --driver-cores 1 ' + _suffix
            logger.warning("driver cores detail is necessaery in blueprint")

        if check_job_settings_key(job_settings, 'driver_memory'):
            _settings += _prefix + ' --driver-memory ' + \
                str(job_settings['driver_memory']) + _suffix
        else:
            _settings += _prefix + ' --driver-memory 2G ' + _suffix
            logger.warning("driver memory detail is necessary in blueprint")

        if check_job_settings_key(job_settings, 'application'):
            _settings += _prefix + " " + \
                str(job_settings['application']) + _suffix
        else:
            logger.error("Application jar is mandatory for running spark app")

        if check_job_settings_key(job_settings, 'application_params'):
            for aps in job_settings['application_params']:
                _settings += _prefix + " " + \
                    str(aps) + _suffix
        else:
            logger.error("Application jar is mandatory for running spark app")

        if job_id:
            _settings += _prefix + " &> ./" + str(job_id) + ".out" + _suffix
        return _settings

# monitor
    def get_states(self, workdir, credentials, job_names, logger):
        states = {}
        frameinfo = getframeinfo(currentframe())
        logger.debug("{2}: {0} - {1}".format(frameinfo.filename,
                                             frameinfo.lineno,
                                             frameinfo.function))
        call = "curl http://{0}:`cat /security/secrets/{0}.mesos" + \
            "`@localhost:5050/frameworks"

        for i in range(5):
            try:
                client = SshClient(credentials)
                user = client._user
            except AuthenticationException as ae:
                logger.debug(ae)
                import time
                time.sleep(5)
                continue

        call_format = call.format(user)
        logger.debug("{2}: cal_fmt: {0}, usr: {1}".format(call_format,
                                                          user,
                                                          frameinfo.function))

        output, exit_code = client.execute_shell_command(call_format,
                                                         workdir=workdir,
                                                         wait_result=True)
        if exit_code == 0:
            json_output = json.loads(output)
            states = self._parse_frameworks_states(json_output,
                                                   job_names[0], logger)
        else:
            logger.warning("failed to get states from {0}".format(
                call_format))

        logger.debug("{0}: job_state:{1}".format(frameinfo.function,
                                                 states))
        client.close_connection()
        return states

    def _parse_frameworks_states(self, frameworks_json, job_name, logger):
        frameinfo = getframeinfo(currentframe())
        logger.debug("{2}: {0} - {1}".format(frameinfo.filename,
                                             frameinfo.lineno,
                                             frameinfo.function))
        """ parse to get state of job by analysing REST JSON """
        parsed = {}
        running_frameworks = []
        completed_frameworks = []
        if 'frameworks' in frameworks_json:
            running_frameworks = frameworks_json['frameworks']
        if 'completed_frameworks' in frameworks_json:
            completed_frameworks = frameworks_json['completed_frameworks']

        # Completed Frameworks details are checked to get state of the jobs
        for framework in completed_frameworks:
            if (framework['name'] == job_name):
                for task in framework['completed_tasks']:
                    if (job_name in parsed):
                        parsed[job_name] = get_prevailing_state(
                            parsed[job_name], task['state'])
                    else:
                        parsed[job_name] = task['state']
        logger.debug('completed_frameworks - parsed:{0}'.format(parsed))

        # Running Frameworks details are checked to get state of the jobs
        for framework in running_frameworks:
            logger.debug('running_framework: {0}'.format(framework))
            logger.debug('r_framework: {0}, job: {1}'.format(
                framework['name'], job_name))
            if (framework['name'] == job_name):
                logger.debug('running_tasks: {0}'.format(
                    framework['tasks']))
                if ((framework['tasks'] == []) and
                        (framework['completed_tasks'] == [])):
                    parsed[job_name] = 'PENDING'
                for task in framework['tasks']:
                    logger.debug('tasks state: {0}'.format(
                        task['state']))
                    if job_name in parsed:
                        parsed[job_name] = get_prevailing_state(
                            parsed[job_name], task['state'])
                    else:
                        parsed[job_name] = task['state']
        logger.debug('running_frameworks - parsed:{0}'.format(parsed))

        if (job_name in parsed):
            if (parsed[job_name] == "TASK_RUNNING"):
                parsed[job_name] = "RUNNING"
            elif (parsed[job_name] == "TASK_FINISHED"):
                parsed[job_name] = "COMPLETED"
            elif (parsed[job_name] == "TASK_KILLED"):
                parsed[job_name] = "CANCELLED"
        else:
            parsed[job_name] = 'FAILED'

        logger.debug('frameworks - parsed:{0}'.format(parsed))
        return parsed
