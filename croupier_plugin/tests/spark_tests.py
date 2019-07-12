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

slurm_tests.py: Holds the Slurm unit tests
'''


import logging
import unittest
import json
import sys
from inspect import currentframe, getframeinfo

from croupier_plugin.workload_managers.workload_manager import WorkloadManager

class TestSpark(unittest.TestCase):
    """ Holds Spark tests """

    def __init__(self, methodName='runTest'):
        super(TestSpark, self).__init__(methodName)
        self.wm = WorkloadManager.factory("SPARK")
        logging.basicConfig(filename='./spark_tests.log',level=logging.DEBUG)
        self.logger = logging.getLogger('TestSpark')

    def test_bad_type_name(self):
        """ Bad name type """
        frameinfo = getframeinfo(currentframe())
        self.logger.debug("we started at {1} - {0}".format(         \
                            frameinfo.lineno, frameinfo.function))
        response = self.wm._build_job_submission_call(42,           \
                                          {'application': 'cmd',    \
                                           'type': 'SPA'},          \
                                          self.logger)
        self.logger.debug("we ended at {1} - {0}".format(         \
                            frameinfo.lineno, frameinfo.function))
        self.assertIn('error', response)

    def test_bad_type_settings(self):
        """ Bad type settings """
        frameinfo = getframeinfo(currentframe())
        self.logger.debug("we started at {1} - {0}".format(         \
                            frameinfo.lineno, frameinfo.function))
        response = self.wm._build_job_submission_call('test',       \
                                                      'bad type',   \
                                                      self.logger)
        self.logger.debug("we ended at {1} - {0}".format(           \
                            frameinfo.lineno, frameinfo.function))
        self.assertIn('error', response)

    def test_bad_settings_command_type(self):
        """ Bad type settings """
        frameinfo = getframeinfo(currentframe())
        self.logger.debug("we started at {1} - {0}".format(         \
                            frameinfo.lineno, frameinfo.function))
        response = self.wm._build_job_submission_call('test',       \
                                                      'bad type',   \
                                                      self.logger)
        self.logger.debug("we ended at {1} - {0}".format(           \
                            frameinfo.lineno, frameinfo.function))
        self.assertIn('error', response)

    def test_empty_settings(self):
        """ Empty job settings """
        frameinfo = getframeinfo(currentframe())
        self.logger.debug("we started at {1} - {0}".format(         \
                            frameinfo.lineno, frameinfo.function))
        response = self.wm._build_job_submission_call('test',       \
                                                      {},           \
                                                      self.logger)
        self.logger.debug("we ended at {1} - {0}".format(           \
                            frameinfo.lineno, frameinfo.function))
        self.assertIn('error', response)

    def test_only_type_settings(self):
        """ Type only as job settings """
        frameinfo = getframeinfo(currentframe())
        self.logger.debug("we started at {1} - {0}".format(         \
                            frameinfo.lineno, frameinfo.function))
        response = self.wm._build_job_submission_call('test',       \
                                      {'application': 'cmd',        \
                                       'type': 'BAD'},              \
                                      self.logger)
        self.assertIn('error', response)

    def test_only_command_settings(self):
        """ Command only as job settings. """
        frameinfo = getframeinfo(currentframe())
        self.logger.debug("we started at {1} - {0}".format(         \
                            frameinfo.lineno, frameinfo.function))
        response = self.wm._build_job_submission_call('test',       \
                                      {'application': 'cmd'},       \
                                      self.logger)
        self.logger.debug("we ended at {1} - {0}".format(           \
                            frameinfo.lineno, frameinfo.function))
        self.assertIn('error', response)

    def test_basic_spark_call(self):
        """ Basic spark-submit command. """
        frameinfo = getframeinfo(currentframe())
        self.logger.debug("we started at {1} - {0}".format(         \
                            frameinfo.lineno, frameinfo.function))
        response = self.wm._build_job_submission_call('test',       \
                              {'type': 'SPARK',                     \
                               'application':                       \
                                 './simple-project_2.11-1.0.jar'},  \
                              self.logger)
        self.assertNotIn('error', response)
        self.assertIn('call', response)

        call = response['call']
        out_req = 'spark-submit --name test ' +                     \
                         ' --total-executor-cores 1' +              \
                         ' --executor-memory 2G' +                  \
                         ' --driver-cores 1' +                      \
                         ' --driver-memory 2G ' +                   \
                         ' ./simple-project_2.11-1.0.jar; &'
        self.logger.debug("we ended at {1} - {0}".format(           \
                            frameinfo.lineno, frameinfo.function))
        self.assertEqual(call.replace(" ", ""),                     \
                         out_req.replace(" ", ""))

    def test_complete_spark_call(self):
        """ Complete spark-submit command. """
        frameinfo = getframeinfo(currentframe())
        self.logger.debug("we started at {1} - {0}".format(         \
                            frameinfo.lineno, frameinfo.function))
        response = self.wm._build_job_submission_call('test',       \
                              {'total_executor_cores': 20,          \
                               'type': 'SPARK',                     \
                               'executor_memory': '2G',             \
                               'driver_memory': '2G',               \
                               'driver_cores': 10,                  \
                               'application':                       \
                                  './simple-project_2.11-1.0.jar'   \
                               },                                   \
                              self.logger)
        self.assertNotIn('error', response)
        self.assertIn('call', response)

        call = response['call']
        self.logger.debug("we ended at {1} - {0}".format(           \
                            frameinfo.lineno, frameinfo.function))
        self.assertEqual(call, 'spark-submit --name test ' +        \
                         ' --total-executor-cores 20' +             \
                         ' --executor-memory 2G' +                  \
                         ' --driver-cores 10' +                     \
                         ' --driver-memory 2G' +                    \
                         ' ./simple-project_2.11-1.0.jar; &')

    def test_random_name(self):
        """ Random name formation. """
        frameinfo = getframeinfo(currentframe())
        self.logger.debug("we started at {1} - {0}".format(         \
                            frameinfo.lineno, frameinfo.function))
        name = self.wm._get_random_name('base')

        self.logger.debug("we ended at {1} - {0}".format(           \
                            frameinfo.lineno, frameinfo.function))
        self.assertEqual(11, len(name))
        self.assertEqual('base_', name[:5])

    def test_random_name_uniqueness(self):
        """ Random name uniqueness. """
        frameinfo = getframeinfo(currentframe())
        self.logger.debug("we started at {1} - {0}".format(         \
                            frameinfo.lineno, frameinfo.function))
        names = []
        for _ in range(0, 50):
            names.append(self.wm._get_random_name('base'))
        self.logger.debug("we ended at {1} - {0}".format(           \
                            frameinfo.lineno, frameinfo.function))
        self.assertEqual(len(names), len(set(names)))

    def test_parse_frameworks_states(self):
        """ Parse state from framework JSON details """
        frameinfo = getframeinfo(currentframe())
        self.logger.debug("we started at {1} - {0}".format(         \
                            frameinfo.lineno, frameinfo.function))
        test_string = '{'                                           +\
                        '"frameworks": ['                           +\
                            '{"name": "Simple Application",'        +\
                              '"tasks": ['                          +\
                                  '{'                               +\
                                    '"state": "TASK_RUNNING"'       +\
                                  '}'                               +\
                              ']'                                   +\
                            '}'                                     +\
                        '],'                                        +\
                        '"completed_frameworks": ['                 +\
                            '{"name": "Simple Application",'        +\
                              '"tasks": ['                          +\
                                  '{'                               +\
                                    '"state": "TASK_FINISHED"'      +\
                                  '}'                               +\
                              ']'                                   +\
                            '}'                                     +\
                        ']'                                         +\
                    '}'
        json_output  = json.loads(test_string)
        parsed = self.wm._parse_frameworks_states(                  \
                            json_output,                            \
                            "Simple Application",                   \
                            self.logger)
        self.assertDictEqual( parsed,                               \
                {"Simple Application": "TASK_RUNNING"} )

        json_output  = json.loads(test_string)
        parsed = self.wm._parse_frameworks_states(                  \
                            json_output,                            \
                            "Simple",                               \
                            self.logger)
        self.assertDictEqual(parsed, {})

        test_string = '{}'
        json_output  = json.loads(test_string)
        parsed = self.wm._parse_frameworks_states(                  \
                            json_output,                            \
                            "Simple Application",                   \
                            self.logger)
        self.logger.debug("we ended at {1} - {0}".format(           \
                            frameinfo.lineno, frameinfo.function))
        self.assertDictEqual(parsed, {})


if __name__ == '__main__':
    unittest.main()
