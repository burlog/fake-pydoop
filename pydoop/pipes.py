#!/usr/bin/env python

from hadut import MContext
from hadut import RContext
from hadut import PipesRunner
from sequencefile.io.SequenceFile import Reader


class Mapper(object):
    def __init__(self, ctx):
        super(Mapper, self).__init__()

class Reducer(object):
    def __init__(self, ctx):
        super(Reducer, self).__init__()

class Combiner(object):
    def __init__(self, ctx):
        super(Combiner, self).__init__()

class TaskContext(object):
    def __init__(self, result = None):
        super(TaskContext, self).__init__()
        self.result = result

    def emit(self, ekey, evalue):
        self.result.emit(ekey, evalue)

class MapContext(TaskContext):
    def __init__(self, entry = None, result = None):
        super(MapContext, self).__init__(result = result)
        self.key = entry[0] if entry else None
        self.value = entry[1] if entry else None

    def getInputKey(self):
        return self.key

    def getInputValue(self):
        return self.value.getBytes()

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

#===============================

class CopyReducer(Reducer):
    def __init__(self, ctx):
        super(CopyReducer, self).__init__(ctx)

    def reduce(self, context):
        while context.nextValue():
            context.emit(context.getInputKey(), context.getInputValue())

class InputReader(object):
    def __init__(self, filename):
        super(InputReader, self).__init__()
        self.reader = Reader(filename)
        self.key = self.reader.getKeyClass()()
        self.value = self.reader.getValueClass()()

    def pos(self):
        return self.reader.getPosition()

    def __iter__(self):
        while self.reader.next(self.key, self.value):
            yield str(self.key), self.value

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
        for i in range(0, count):
            self.map_tasks.append([mapper_class(MapContext()), ResultStore()])

    def __iter__(self):
        for map_task, result_collector in self.map_tasks:
            yield map_task, result_collector

class Factory(object):
    def __init__(self, mapper_class, reducer_class, combiner_class = None):
        super(Factory, self).__init__()
        self.tasks = 3
        self.input_filename = PipesRunner.CONTEXT["fake.pydoop.input.file"]
        self.mapper_class = mapper_class
        self.reducer_class = reducer_class
        self.combiner_class = combiner_class
        if not self.combiner_class: self.combiner_class = CopyReducer

    def open_sqf(self):
        return InputReader(self.input_filename)

    def make_map_tasks(self):
        return MapRunner(self.tasks, self.mapper_class)

    def make_combine_task(self):
        return self.combiner_class(ReduceContext())

    def make_reduce_task(self):
        return self.reducer_class(ReduceContext())

def runTask(factory):
    sqf = factory.open_sqf()
    map_tasks = factory.make_map_tasks()

    # map phase
    try:
        i = 0
        ientry = sqf.__iter__()
        while True:
            for map_task, map_result in map_tasks:
                entry = ientry.next()
                print("Mapping key: %s [at=%s]" % (entry[0], sqf.pos()))
                map_task.map(map_result.create_map_context(entry))
                i += 1
                if i > 3000: raise StopIteration()
    except StopIteration: pass

    # combiner phase
    combiners_result = ResultStore()
    for _, map_result in map_tasks:
        combine_task = factory.make_combine_task()
        for entry in map_result:
            print("Combine key: %s [values=%s]" % (entry[0], len(entry[1])))
            combine_task.reduce(combiners_result.create_combine_context(entry))
            #print(map_result.store)

    # last reduce phase
    reduce_result = ResultStore()
    reduce_task = factory.make_reduce_task()
    for entry in combiners_result:
        print("Reduce key: %s [values=%s]" % (entry[0], len(entry[1])))
        reduce_task.reduce(reduce_result.create_reduce_context(entry))

    # print output out
    for key in sorted(reduce_result.store.keys()):
        print("%s: %s" % (key, reduce_result.store[key].store))

