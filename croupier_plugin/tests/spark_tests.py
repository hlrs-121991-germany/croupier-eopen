"""

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

spark_tests.py: Holds the Spark unit tests

"""


import logging
import unittest
import json
import os
from inspect import currentframe, getframeinfo

from croupier_plugin.workload_managers.workload_manager import WorkloadManager


class TestSpark(unittest.TestCase):
    """ Holds Spark tests """

    def __init__(self, methodName='runTest'):
        super(TestSpark, self).__init__(methodName)
        logging.basicConfig(filename='./spark_tests.log',
                            level=logging.DEBUG)
        self.logger = logging.getLogger('TestSpark')
        self.wm = WorkloadManager.factory("SPARK")

    def test_build_container_script(self):
        """ Container script is not supported in the SPARK """
        frameinfo = getframeinfo(currentframe())
        self.logger.warning("{1} : {0}".format(frameinfo.lineno,
                                               frameinfo.function))
        job_settings = {'application': 'cmd', 'type': 'SPARK'}
        response = self.wm._build_container_script('test', job_settings,
                                                   self.logger)
        self.assertIsNone(response)

    def test_bad_application_name(self):
        """ Bad name type """
        frameinfo = getframeinfo(currentframe())
        self.logger.warning("{1} : {0}".format(frameinfo.lineno,
                                               frameinfo.function))
        job_settings = {'application': 'cmd', 'type': 'SPARK'}
        app_name = 42  # app_name should be in string
        response = self.wm._build_job_submission_call(app_name, job_settings,
                                                      self.logger)
        self.assertIn('error', response)

    def test_bad_job_settings(self):
        """ Bad type settings """
        frameinfo = getframeinfo(currentframe())
        self.logger.warning("{1} : {0}".format(frameinfo.lineno,
                                               frameinfo.function))
        job_settings = 'bad type'  # Job setting type is Dictionary
        response = self.wm._build_job_submission_call('test', job_settings,
                                                      self.logger)
        self.assertIn('error', response)

        # Empty job settings
        response = self.wm._build_job_submission_call('test', {}, self.logger)
        self.assertIn('error', response)

        # Bad job type in the job_settings
        job_settings = {'application': 'cmd', 'type': 'SPA'}
        response = self.wm._build_job_submission_call('test', job_settings,
                                                      self.logger)
        self.assertIn('error', response)

        # Previously Workload Manager type is SPARK, so you shoud not
        # give 'type': 'SLURM' afterwards
        job_settings = {'application': 'cmd', 'type': 'SLURM'}
        response = self.wm._build_job_submission_call('test', job_settings,
                                                      self.logger)
        self.assertIn('error', response)
        job_settings = {'application': 'cmd', 'type': 'spark'}
        response = self.wm._build_job_submission_call('test', job_settings,
                                                      self.logger)
        self.assertIn('error', response)

        # Command only in the job settings
        job_settings = {'application': 'cmd'}
        response = self.wm._build_job_submission_call('test', job_settings,
                                                      self.logger)
        self.assertIn('error', response)

    def test_basic_spark_call(self):
        """ Basic spark-submit command. """
        frameinfo = getframeinfo(currentframe())
        self.logger.warning("{1} : {0}".format(frameinfo.lineno,
                                               frameinfo.function))
        app_name = 'test'
        job_settings = {'type': 'SPARK', 'application':
                        './simple-project_2.11-1.0.jar'}
        response = self.wm._build_job_submission_call(app_name, job_settings,
                                                      self.logger)
        self.assertNotIn('error', response)
        self.assertIn('call', response)

        call = response['call']
        out_req = 'nohup sh spark-submit --name test ' +    \
            ' --total-executor-cores 1' +                   \
            ' --executor-memory 2G' +                       \
            ' --driver-cores 1' +                           \
            ' --driver-memory 2G ' +                        \
            ' ./simple-project_2.11-1.0.jar &>./test.out &'
        self.assertEqual(call.replace(" ", ""), out_req.replace(" ", ""))

        # Complete spark-submit command
        frameinfo = getframeinfo(currentframe())
        self.logger.warning("{1} : {0}".format(frameinfo.lineno,
                                               frameinfo.function))
        job_settings = {'type': 'SPARK', 'type': 'SPARK', 'application':
                        './simple-project_2.11-1.0.jar',
                        'executor_memory': '2G', 'driver_memory': '2G',
                        'driver_cores': 10, 'total_executor_cores': 20}
        response = self.wm._build_job_submission_call('test', job_settings,
                                                      self.logger)
        self.assertNotIn('error', response)
        self.assertIn('call', response)

        call = response['call']
        out_req = 'nohup sh spark-submit ' +            \
            ' --name test --total-executor-cores 20' +  \
            ' --executor-memory 2G' +                   \
            ' --driver-cores 10' +                      \
            ' --driver-memory 2G' +                     \
            ' ./simple-project_2.11-1.0.jar &>' +       \
            ' ./test.out &'
        self.assertEqual(call.replace(" ", ""), out_req.replace(" ", ""))

    def test_random_name(self):
        """ Random name formation. """
        frameinfo = getframeinfo(currentframe())
        self.logger.warning("{1} : {0}".format(frameinfo.lineno,
                                               frameinfo.function))
        name = self.wm._get_random_name('base')

        self.assertEqual(11, len(name))
        self.assertEqual('base_', name[:5])

    def test_random_name_uniqueness(self):
        """ Random name uniqueness. """
        frameinfo = getframeinfo(currentframe())
        self.logger.warning("{1} : {0}".format(frameinfo.lineno,
                                               frameinfo.function))
        names = []
        for _ in range(0, 50):
            names.append(self.wm._get_random_name('base'))
        self.assertEqual(len(names), len(set(names)))

    def test_parse_frameworks_states(self):
        """ Parse state from framework JSON details """
        frameinfo = getframeinfo(currentframe())
        self.logger.warning("{1} : {0}".format(frameinfo.lineno,
                                               frameinfo.function))
        json_input = {}
        with open(os.path.join(os.path.dirname(__file__),
                               'input_spark_pretty.json')) as json_file:
            json_input = json.load(json_file)

        parsed = self.wm._parse_frameworks_states(json_input,
                                                  "SA_HPDA_HLRS849j32",
                                                  self.logger)
        self.assertDictEqual(parsed, {"SA_HPDA_HLRS849j32": "RUNNING"})

        parsed = self.wm._parse_frameworks_states(json_input,
                                                  "Simple",
                                                  self.logger)
        self.assertDictEqual(parsed, {"Simple": "FAILED"})

        parsed = self.wm._parse_frameworks_states(json_input,
                                                  "SA_HPDA_HLRS849j34",
                                                  self.logger)
        self.assertDictEqual(parsed, {'SA_HPDA_HLRS849j34': 'PENDING'})

        json_input = {}
        parsed = self.wm._parse_frameworks_states(json_input,
                                                  "SA_HPDA_HLRS849j34",
                                                  self.logger)
        self.assertDictEqual(parsed, {'SA_HPDA_HLRS849j34': 'FAILED'})

    def test_parse_running_frameworks(self):
        """ Parse state from running framework JSON details """
        frameinfo = getframeinfo(currentframe())
        self.logger.warning("{1} : {0}".format(frameinfo.lineno,
                                               frameinfo.function))
        json_input = {}
        with open(os.path.join(os.path.dirname(__file__),
                               'input_spark_pretty.json')) as json_file:
            json_input = json.load(json_file)
        user = "hpcdraja"
        out_fwk_id = "a1e7fe28-64f7-4f81-b794-df8963b3b09e-0224"
        job_name = "SA_HPDA_HLRS849j32"
        parsed = self.wm._parse_running_frameworks(json_input["frameworks"],
                                                   user, job_name, self.logger)
        spark_cancel_fmt = "curl -X POST http://{1}:`cat /security/" +      \
            "secrets/{1}.mesos`@localhost:5050/teardown -d 'frameworkId={0}'; "
        spark_cancl = spark_cancel_fmt.format(out_fwk_id, user)
        self.assertEqual(parsed.replace(" ", ""), spark_cancl.replace(" ", ""))


if __name__ == '__main__':
    unittest.main()
