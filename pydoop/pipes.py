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
from pydoop.sequencefile.util.ReflectionUtils import hadoopClassFromName

class RecordReader(object):
    def __init__(self, ctx):
        super(RecordReader, self).__init__()

class RecordWriter(object):
    def __init__(self, ctx):
        super(RecordWriter, self).__init__()

class Partitioner(object):
    def __init__(self, ctx):
        super(Partitioner, self).__init__()

class Mapper(object):
    def __init__(self, ctx):
        super(Mapper, self).__init__()

    def close(self):
        raise NotImplementedError()

    def map(self, ctx):
        raise NotImplementedError()

class Reducer(object):
    def __init__(self, ctx):
        super(Reducer, self).__init__()

    def close(self):
        raise NotImplementedError()

    def reduce(self, ctx):
        raise NotImplementedError()

class Combiner(object):
    def __init__(self, ctx):
        super(Combiner, self).__init__()

    def close(self):
        raise NotImplementedError()

    def reduce(self, ctx):
        raise NotImplementedError()

class TaskContext(object):
    def __init__(self, result = None):
        super(TaskContext, self).__init__()
        self.result = result

    def emit(self, ekey, evalue):
        if type("") != type(ekey):
            raise AttributeError("emited key is not str")
        if type("") != type(evalue):
            raise AttributeError("emited value is not str")
        self.result.emit(ekey, evalue)

    def getJobConf(self):
        return JobConf()

    def getInputKey(self):
        raise NotImplementedError()

    def getInputValue(self):
        raise NotImplementedError()

    def progress(self):
        raise NotImplementedError()

    def setStatus(self, status):
        pass

    def getCounter(self, group, name):
        return Counter()

    def incrementCounter(self, counter, amount):
        pass

class MapContext(TaskContext):
    def __init__(self, entry = None, result = None):
        super(MapContext, self).__init__(result = result)
        self.key = entry[0] if entry else None
        self.value = entry[1] if entry else None

    def getInputKey(self):
        return self.key

    def getInputValue(self):
        return self.value.getBytes()

    def getInputSplit(self):
        raise NotImplementedError()

    def getInputKeyClass(self):
        raise NotImplementedError()

    def getInputValueClass(self):
        raise NotImplementedError()

class ReduceContext(TaskContext):
    def __init__(self, entry = None, result = None):
        super(ReduceContext, self).__init__(result = result)
        self.key = entry[0] if entry else None
        self.value = entry[1] if entry else None

    def getInputKey(self):
        return self.key

    def getInputValue(self):
        return self.value.current()

    def nextValue(self):
        return self.value.nextValue()

class JobConf(object):
    def __init__(self):
        super(JobConf, self).__init__()

    def hasKey(self, key):
        return key in os.environ

    def get(self, key):
        return str(os.environ.get(key))

    def getInt(self, key):
        return int(os.environ.get(key))

    def getFloat(self, key):
        return float(os.environ.get(key))

    def getBoolean(self, key):
        return bool(os.environ.get(key))

class Counter(object):
    def __init__(self):
        super(Counter, self).__init__()

    def getId(self):
        raise NotImplementedError()

class CopyReducer(Reducer):
    def __init__(self, ctx):
        super(CopyReducer, self).__init__(ctx)

    def reduce(self, context):
        while context.nextValue():
            context.emit(context.getInputKey(), context.getInputValue())

class InputReader(object):
    def __init__(self, filename):
        super(InputReader, self).__init__()
        self.reader_class_name = "org.apache.hadoop.mapred.TextInputFormat"
        if "mapred.input.format.class" in os.environ:
            self.reader_class_name = os.environ["mapred.input.format.class"]
        self.reader_class = hadoopClassFromName(self.reader_class_name)
        self.reader = self.reader_class(filename)
        self.key = self.reader.getKeyClass()()
        self.value = self.reader.getValueClass()()

    def pos(self):
        return self.reader.getPosition()

    def next(self):
        if self.reader.next(self.key, self.value):
            return str(self.key), self.value
        return None

class ResultValue(object):
    def __init__(self, value = None):
        super(ResultValue, self).__init__()
        self.store = [value] if value else []
        self.idx = -1

    def append(self, value):
        self.store.append(value)

    def current(self):
        return self.store[self.idx]

    def nextValue(self):
        self.idx += 1
        return self.idx < len(self.store)

    def __len__(self):
        return len(self.store)

    def __str__(self):
        return str(self.store)

class ResultStore(object):
    def __init__(self):
        super(ResultStore, self).__init__()
        self.store = {}

    def create_map_context(self, entry):
        return MapContext(entry = entry, result = self)

    def create_combine_context(self, entry):
        return ReduceContext(entry = entry, result = self)

    def create_reduce_context(self, entry):
        return ReduceContext(entry = entry, result = self)

    def emit(self, ekey, evalue):
        if ekey in self.store:
            self.store[ekey].append(evalue)
        else:
            self.store[ekey] = ResultValue(evalue)

    def __iter__(self):
        for key in sorted(self.store.keys()):
            yield key, self.store[key]

    def __str__(self):
        return str(self.store)

class MapRunner(object):
    def __init__(self, count, mapper_class):
        super(MapRunner, self).__init__()
        self.map_tasks = []
        if count == 0:
            result = ResultStore()
            for i in range(0, 3):
                mapper = mapper_class(MapContext())
                self.map_tasks.append([mapper, result])
        else:
            for i in range(0, count):
                mapper = mapper_class(MapContext())
                result = ResultStore()
                self.map_tasks.append([mapper, result])

    def __iter__(self):
        for map_task, result_collector in self.map_tasks:
            yield map_task, result_collector

class Factory(object):
    def __init__(self,
                 mapper_class,
                 reducer_class,
                 record_reader_class = None,
                 record_writer_class = None,
                 combiner_class = None,
                 partitioner_class = None):
        super(Factory, self).__init__()
        self.tasks = int(os.environ.get("mapred.reduce.tasks", 3))
        self.input_filename = os.environ.get("fake.pydoop.input.file", "")
        self.output_filename = os.environ.get("fake.pydoop.output.file", "")
        self.mapper_class = mapper_class
        self.reducer_class = reducer_class
        self.combiner_class = combiner_class
        if not self.combiner_class: self.combiner_class = CopyReducer

    def open_reader(self):
        return InputReader(self.input_filename)

    def make_map_tasks(self):
        return MapRunner(self.tasks, self.mapper_class)

    def make_combine_task(self):
        return self.combiner_class(ReduceContext())

    def make_reduce_task(self):
        return self.reducer_class(ReduceContext())

def runTask(factory):
    file_reader = factory.open_reader()
    map_tasks = factory.make_map_tasks()
    print "Output file is:", factory.output_filename

    # map phase
    i = 0
    stop = False
    limit = int(os.environ.get("fake.pydoop.input.records.limit", -1))
    while not stop:
        for map_task, map_result in map_tasks:
            entry = file_reader.next()
            if not entry:
                stop = True
                break
            print("Mapping key: %s [#=%s, at=%s]"
                  % (entry[0], i, file_reader.pos()))
            map_task.map(map_result.create_map_context(entry))
            i += 1
            if limit != -1 and i > limit:
                stop = True
                break

    # print map result if there are no reduce tasks
    if factory.tasks == 0:
        _, map_result = map_tasks.__iter__().next()
        for key in sorted(map_result.store.keys()):
            print("%s: %s" % (key, map_result.store[key].store))
        return

    # combiner phase
    combiners_result = ResultStore()
    for _, map_result in map_tasks:
        combine_task = factory.make_combine_task()
        for entry in map_result:
            if factory.combiner_class != CopyReducer:
                print("Combine key: %s [values=%s]" % (entry[0], len(entry[1])))
            combine_task.reduce(combiners_result.create_combine_context(entry))

    # last reduce phase
    reduce_result = ResultStore()
    reduce_task = factory.make_reduce_task()
    for entry in combiners_result:
        print("Reduce key: %s [values=%s]" % (entry[0], len(entry[1])))
        reduce_task.reduce(reduce_result.create_reduce_context(entry))

    # print output on the screen
    f = open(factory.output_filename,'w')
    for key in sorted(reduce_result.store.keys()):
        for s in reduce_result.store[key].store:
            print("%s\t%s" % (key,s))
            f.write("%s\t%s\n" % (key,s))
    f.close()

