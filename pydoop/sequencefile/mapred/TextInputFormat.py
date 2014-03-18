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

import sys
from pydoop.sequencefile.util.ReflectionUtils import hadoopClassFromName

class TextInputFormat(object):
    def __init__(self, filename):
        super(TextInputFormat, self).__init__()
        self._value_class = None
        self._key_class = None
        if filename == "" or filename == "-":
            self._stream = sys.stdin
        else:
            self._stream = open(filename, "r")
        self.pos = 0

    def close(self):
        pass

    def getKeyClass(self):
        if not self._key_class:
            self._key_class = hadoopClassFromName("hadoop.io.Text")
        return self._key_class

    def getKeyClassName(self):
        return hadoopClassName(self.getKeyClass())

    def getValueClass(self):
        if not self._value_class:
            self._value_class = hadoopClassFromName("hadoop.io.Text")
        return self._value_class

    def getValueClassName(self):
        return hadoopClassName(self.getValueClass())

    def getPosition(self):
        return self.pos

    def next(self, key, value):
        line = self._stream.readline()
        if line == "": return False
        self.pos += 1
        key.setFields(str(self.pos))
        value.setFields(line)
        return True

