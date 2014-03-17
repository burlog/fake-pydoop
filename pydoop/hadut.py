#!/usr/bin/env python

class RContext(object):
    def __init__(self, properties):
        self.properties = properties
        self.idx = 0
        self.key = None
        self.value = None
        self.writer = None
        self.partition = {}

    def consume(self, ekey, evalue):
        if ekey not in self.partition:
            self.partition[ekey] = [evalue]
        else:
            self.partition[ekey].append(evalue)

    def getInputKey(self):
        return self.key

    def getInputValue(self):
        return self.value[self.idx]

    def nextValue(self):
        self.idx += 1
        if self.idx >= len(self.value): return False
        return True

    def emit(self, ekey, evalue):
        print(ekey, evalue)

    def __iter__(self):
        for ekey in sorted(self.partition.keys()):
            self.key = ekey
            self.value = self.partition[ekey]
            self.idx = -1
            yield ekey

    CONTEXT = None

class CContext(object):
    def __init__(self, properties):
        self.properties = properties
        self.idx = 0
        self.key = None
        self.value = None
        self.writer = None
        self.partition = {}

    def consume(self, ekey, evalue):
        if ekey not in self.partition:
            self.partition[ekey] = [evalue]
        else:
            self.partition[ekey].append(evalue)

    def getInputKey(self):
        return self.key

    def getInputValue(self):
        return self.value[self.idx]

    def nextValue(self):
        self.idx += 1
        if self.idx >= len(self.value): return False
        return True

    def emit(self, ekey, evalue):
        print(ekey, evalue)

    def __iter__(self):
        for ekey in sorted(self.partition.keys()):
            self.key = ekey
            self.value = self.partition[ekey]
            self.idx = -1
            yield ekey

class MContext(object):
    def __init__(self, properties):
        self.properties = properties
        self.reader = None
        self.combiner = CContext(properties)

    def open_input(self, input_):
        self.reader = Reader(input_)
        self.key = self.reader.getKeyClass()()
        self.value = self.reader.getValueClass()()

    def next(self):
        if self.reader.next(self.key, self.value):
            return self.key, self.value
        return None, None

    def getInputKey(self):
        return self.key

    def getInputValue(self):
        return self.value.getBytes()

    def pos(self):
        return self.reader.getPosition()

    def emit(self, ekey, evalue):
        self.combiner.consume(ekey, evalue)
        print("Map emits: %s" % str([ekey, evalue]))

    def __iter__(self):
        while self.reader.next(self.key, self.value):
            yield self.key, self.value

    CONTEXT = None

class PipesRunner:
    def __init__(self, prefix = None):
        pass

    def set_input(self, filename):
        self.filename = filename

    def set_exe(self, exe):
        self.exe = exe

    def run(self, properties = None, hadoop_conf_dir = None, logger = None):
        # prepare global context
        PipesRunner.CONTEXT = properties.copy()
        PipesRunner.CONTEXT["fake.pydoop.input.file"] = self.filename

        # exec the runner script
        gdict = globals()
        gdict["__name__"] = "__main__"
        ldict = locals()
        exec self.exe in gdict, ldict

    CONTEXT = None

