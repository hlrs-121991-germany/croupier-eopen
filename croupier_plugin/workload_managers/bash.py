'''
Copyright (c) 2019 Atos Spain SA. All rights reserved.

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

@author: Javier Carnero
         Atos Research & Innovation, Atos Spain S.A.
         e-mail: javier.carnero@atos.net

bash.py
'''


from croupier_plugin.ssh import SshClient
from croupier_plugin.workload_managers import workload_manager


class Bash(workload_manager.WorkloadManager):

    def _build_job_submission_call(self, name, job_settings, logger):
        # check input information correctness
        if not isinstance(job_settings, dict) or not isinstance(name,
                                                                basestring):
            return {'error': "Incorrect inputs"}

        if 'type' not in job_settings or 'command' not in job_settings:
            return {'error': "'type' and 'command' " +
                    "must be defined in job settings"}

        if job_settings['type'] != 'SHELL':
            return {'error': "Job type '" + job_settings['type'] +
                    "'not supported"}

        # Build single line command
        bash_call = ''

        # NOTE an uploaded script could also be interesting to execute
        if 'pre' in job_settings:
            for entry in job_settings['pre']:
                bash_call += entry + '; '

        # add executable and arguments, and save exit code on env
        bash_call += job_settings['command'] + '; '
        bash_call += 'echo ' + name + ',$? >> msomonitor.data; '

        # NOTE an uploaded script could also be interesting to execute
        if 'post' in job_settings:
            for entry in job_settings['post']:
                bash_call += entry + '; '

        # Run in the background detached from terminal
        bash_call = 'nohup sh -c "' + bash_call + '" &'

        response = {}
        response['call'] = bash_call
        return response

    def _build_job_cancellation_call(self, name, job_settings, logger):
        return "pkill -f " + name

# Monitor
    def get_states(self, workdir, credentials, job_names, logger):
        # TODO set start time of consulting
        # (sacct only check current day)
        call = "cat msomonitor.data"

        client = SshClient(credentials)

        output, exit_code = client.execute_shell_command(
            call,
            workdir=workdir,
            wait_result=True)

        client.close_connection()

        states = {}
        if exit_code == 0:
            states = self._parse_states(output, logger)

        return states

    def _parse_states(self, raw_states, logger):
        """ Parse two colums exit codes into a dict """
        jobs = raw_states.splitlines()
        parsed = {}
        if jobs and (len(jobs) > 1 or jobs[0] != ''):
            for job in jobs:
                first, second = job.strip().split(',')
                parsed[first] = self._parse_exit_codes(second)

        return parsed

    def _parse_exit_codes(self, exit_code):
        if exit_code == '0':  # exited normally
            return workload_manager.JOBSTATESLIST[workload_manager.COMPLETED]
        elif exit_code == '1':  # general error
            return workload_manager.JOBSTATESLIST[workload_manager.FAILED]
        elif exit_code == '126':  # cannot execute
            return workload_manager.JOBSTATESLIST[workload_manager.REVOKED]
        elif exit_code == '127':  # not found
            return workload_manager.JOBSTATESLIST[workload_manager.BOOTFAIL]
        elif exit_code == '130':  # terminated by ctrl+c
            return workload_manager.JOBSTATESLIST[workload_manager.CANCELLED]
        else:
            return workload_manager.JOBSTATESLIST[workload_manager.FAILED]
