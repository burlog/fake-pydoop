#!/usr/bin/env python2
# ========================================================================
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

class PipesRunner:
    def __init__(self, prefix = None):
        self.filename = None
        self.output = "/dev/null" # FIXME

    def set_input(self, filename, put = None):
        self.filename = filename
        os.environ["fake.pydoop.input.file"] = self.filename

    def set_output(self, output):
        raise NotImplementedError()

    def clean(self):
        pass

    def collect_output(out_file = None):
        raise NotImplementedError()

    def set_exe(self, exe):
        self.exe = exe

    def run(self, properties = None, hadoop_conf_dir = None, logger = None):
        for key in properties.keys():
            os.environ[key] = str(properties[key])
        gdict = globals()
        gdict["__name__"] = "__main__"
        ldict = locals()
        exec self.exe in gdict, ldict

def collect_output(mr_out_dir, out_file = None):
    raise NotImplementedError()

def dfs(args = None, properties = None, hadoop_conf_dir = None):
    raise NotImplementedError()

def find_jar(jar_name, root_path = None):
    raise NotImplementedError()

def get_num_nodes(properties = None, hadoop_conf_dir = None, offline = False):
    raise NotImplementedError()

def get_task_trackers(properties = None,
                      hadoop_conf_dir = None,
                      offline = False):
    raise NotImplementedError()

def path_exists(path, properties = None, hadoop_conf_dir = None):
    raise NotImplementedError()

def run_class(class_name,
              args = None,
              properties = None,
              classpath = None,
              hadoop_conf_dir = None,
              logger = None):
    raise NotImplementedError()

def run_cmd(cmd,
            args = None,
            properties = None,
            hadoop_home = None,
            hadoop_conf_dir = None,
            logger = None):
    raise NotImplementedError()

def run_jar(jar_name,
            more_args = None,
            properties = None,
            hadoop_conf_dir = None):
    raise NotImplementedError()

def run_pipes(executable,
              input_path,
              output_path,
              more_args = None,
              properties = None,
              force_pydoop_submitter = False,
              hadoop_conf_dir = None,
              logger = None):
    raise NotImplementedError()

