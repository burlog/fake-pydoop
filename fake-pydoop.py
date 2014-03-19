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

import imp, sys, os
from subprocess import call

def is_exe(filename):
    return os.path.isfile(filename) and os.access(filename, os.X_OK)

def print_usage(out, msg = None):
    if msg: out.write(msg + "\n")
    out.write("Usage: fake-pydoop.py [fake-pydoop-options] "
              "/path/to/mr/job.py  [mr-job-options]\n\n")
    out.write("fake-pydoop-options:\n"
              " --fp-input-records-limit=LIMIT\t"
                  "how many records is read from input file\n")

def parse_args(argv):
    if len(argv) == 0: return argv
    i = 0
    while i < len(argv):
        if argv[i] == "--help" or argv[i] == "-h":
            print_usage(sys.stdout)
            sys.exit(0)
        elif argv[i] == "--fp-input-records-limit":
            i += 1
            if i >= len(argv) or not argv[i].isdigit():
                print_usage(sys.stderr,
                            "--fp-input-records-limit expects numeric param")
                sys.exit(1)
            os.environ["fake.pydoop.input.records.limit"] = argv[i]
            i += 1
            continue
        break
    return argv[i], argv[i + 1:]

def main():
    filename, argv = parse_args(sys.argv[1:])
    PYTHONPATH = os.environ.get("PYTHONPATH", "")
    os.environ["PYTHONPATH"] = PYTHONPATH                                      \
                             + os.path.dirname(filename)                       \
                             + ":" + os.path.dirname(sys.argv[0])
    if is_exe(filename):
        os.execv(filename, [filename] + argv)
    os.execv("/usr/bin/env", ["/usr/bin/env"] + ["python2"] + [filename] + argv)

if __name__ == "__main__":
    main()
