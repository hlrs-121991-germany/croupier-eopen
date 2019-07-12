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
from time import gmtime, strftime
from inspect import currentframe, getframeinfo
from croupier_plugin.ssh import SshClient
from croupier_plugin.workload_managers.workload_manager import (
    WorkloadManager,
    get_prevailing_state)


class Spark(WorkloadManager):
    """ Spark Workload Manger Driver """

    def _build_container_script(self, name, job_settings, logger):
        frameinfo = getframeinfo(currentframe())
        logger.debug("we at {0} - {2} - {1}".format(frameinfo.filename, frameinfo.lineno, frameinfo.function))
        # check input information correctness
        if not isinstance(job_settings, dict) or \
                not isinstance(name, basestring):
            logger.error("Script malformed {0} - {1}".format(frameinfo.filename, frameinfo.lineno))
            return None
        else:
            return "Script not applicable for Spark!!! exception at {0} - {2} - {1}".format( \
                                                  frameinfo.filename, \
                                                  frameinfo.lineno, \
                                                  frameinfo.function)

    def _build_job_submission_call(self, name, job_settings, logger):
        # check input information correctness
        frameinfo = getframeinfo(currentframe())
        logger.debug("we at {0} - {2} - {1}".format(frameinfo.filename, frameinfo.lineno, frameinfo.function))
        if not isinstance(job_settings, dict) or \
                not isinstance(name, basestring):
            return {'error': "Incorrect inputs"}

        if 'type' not in job_settings or 'application' not in job_settings:
            return {'error': "'type' and 'application' " +
                    "must be defined in job settings"}

        if (job_settings['type'] != 'SPARK'):
            return {'error' : "error in 'type' value"}

        # Build single line command to submit jobs through spark_submit
        spark_call = "spark-submit --name " + str(name) + " "
        spark_call += self._parse_spark_job_settings(name,
                                                     job_settings,
                                                     None, None, logger)
        spark_call += "; &"
        response = {'call' : spark_call}
        logger.debug("we at {0}-{2}-{1}:: {3}".format(frameinfo.filename, frameinfo.lineno, frameinfo.function, response))
        return response

    def _build_job_cancellation_call(self, name, ssh_client, logger):
        frameinfo = getframeinfo(currentframe())
        logger.debug("we at {0} - {2} - {1}".format(frameinfo.filename, frameinfo.lineno, frameinfo.function))
        call = "curl -x post http://$user:`cat /security/secrets/$user.mesos`@localhost:5050/frameworks"
        spark_cancel_call = None

        output, exit_code = ssh_client.execute_shell_command(
            call,
            workdir=workdir,
            wait_result=true)

        json_output  = json.loads(output)
        running_frameworks = json_output["frameworks"]
        if exit_code == 0:
            spark_cancel_call = self._parse_running_frameworks(running_frameworks, logger)
        else:
            logger.warning("failed to get states")

        # frameworkId = job_settings['frameworkId'] , Save fameworkID and cancel it accordingly in the future
        logger.debug("we at {0}-{2}-{1}:: {3}".format(frameinfo.filename, frameinfo.lineno, frameinfo.function, spark_cancel_call))
        return spark_cancel_call

    def _parse_running_frameworks(self, running_frameworks, logger):
        frameinfo = getframeinfo(currentframe())
        logger.debug("we at {0} - {2} - {1}".format(frameinfo.filename, frameinfo.lineno, frameinfo.function))
        spark_cancel_call = None
        for run_framework in running_frameworks:
            if run_framework['name'] == name:
                if spark_cancel_call is None:
                    spark_cancel_call = ""
                else:
                    spark_cancel_call = spark_cancel_call + \
                        "curl -X POST http://$USER:`cat /security/secrets/$USER.mesos`@localhost:5050/teardown -d 'frameworkId={0}'; ".format( \
                                        run_framework["id"])

        logger.debug("we at {0}-{2}-{1}".format(frameinfo.filename, frameinfo.lineno, frameinfo.function, spark_cancel_call))
        return spark_cancel_call

    def _parse_spark_job_settings(self, job_id, job_settings, prefix, suffix, logger):
        frameinfo = getframeinfo(currentframe())
        logger.debug("we at {0} - {2} - {1}".format(frameinfo.filename, frameinfo.lineno, frameinfo.function))
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
            logger.warning("executor cores detail is necessaery in the blueprint")

        if check_job_settings_key(job_settings, 'executor_memory'):
            _settings += _prefix + ' --executor-memory ' + \
                str(job_settings['executor_memory']) + _suffix
        else:
            _settings += _prefix + ' --executor-memory 2G ' + _suffix
            logger.warning("executor memory detail is necessary in the blueprint")

        if check_job_settings_key(job_settings, 'driver_cores'):
            _settings += _prefix + ' --driver-cores ' + \
                str(job_settings['driver_cores']) + _suffix
        else:
            _settings += _prefix + ' --driver-cores 1 ' + _suffix
            logger.warning("driver cores detail is necessaery in the blueprint")

        if check_job_settings_key(job_settings, 'driver_memory'):
            _settings += _prefix + ' --driver-memory ' + \
                str(job_settings['driver_memory']) + _suffix
        else:
            _settings += _prefix + ' --driver-memory 2G ' + _suffix
            logger.warning("driver memory detail is necessary in the blueprint")

        if check_job_settings_key(job_settings, 'application'):
            _settings += _prefix + " " + \
                str(job_settings['application']) + _suffix
        else:
            logger.error("Application jar is mandatory for running spark app")

        logger.debug("we at {0}-{2}-{1}:: {3}".format(frameinfo.filename, frameinfo.lineno, frameinfo.function, _settings))
        return _settings

# monitor
    def get_states(self, workdir, credentials, job_names, logger):
        frameinfo = getframeinfo(currentframe())
        logger.debug("we at {0} - {2} - {1}".format(frameinfo.filename, frameinfo.lineno, frameinfo.function))
        call = "curl -x post http://$user:`cat /security/secrets/$user.mesos`@localhost:5050/frameworks"

        client = sshclient(credentials)

        output, exit_code = client.execute_shell_command(
            call,
            workdir=workdir,
            wait_result=true)

        client.close_connection()

        states = {}
        if exit_code == 0:
            json_output  = json.loads(output)
            states = self._parse_frameworks_states(json_output, job_names, logger)
        else:
            logger.warning("failed to get states")

        logger.debug("we at {0}-{2}-{1}:: {3}".format(frameinfo.filename, frameinfo.lineno, frameinfo.function, states))
        return states

    def _parse_frameworks_states(self, frameworks_json, job_name, logger):
        frameinfo = getframeinfo(currentframe())
        logger.debug("we at {0} - {2} - {1}".format(frameinfo.filename, frameinfo.lineno, frameinfo.function))
        """ parse to get state of job by analysing tasks in the spark framework """
        parsed = {}
        if 'frameworks' in frameworks_json:
            running_frameworks = frameworks_json['frameworks']
        else:
            running_frameworks = []
        if 'completed_frameworks' in frameworks_json:
            completed_frameworks = frameworks_json['completed_frameworks']
        else:
            completed_frameworks = []
        # Completed Frameworks details are checked to get state of the jobs
        for framework in completed_frameworks:
            if (framework['name'] == job_name) and ('completed_tasks' in framework):
                for task in framework['completed_tasks']:
                    if job_name in parsed:
                        parsed[job_name] = get_prevailing_state(parsed[first], task['state'])
                    else:
                        parsed[job_name] = task['state']

        # Running Frameworks details are checked to get state of the jobs
        for framework in running_frameworks:
            if (framework['name'] == job_name) and ('tasks' in framework):
                for task in framework['tasks']:
                    if job_name in parsed:
                        parsed[job_name] = get_prevailing_state(parsed[first], task['state'])
                    else:
                        parsed[job_name] = task['state']
        logger.debug("we at {0}-{2}-{1}:: {3}".format(frameinfo.filename, frameinfo.lineno, frameinfo.function, parsed))
        return parsed
