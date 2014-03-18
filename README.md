Fake-Pydoop
===========

Pure python fake pydoop implementation for testing purposes. You can run your
map reduce pydoop task without hadoop and pydoop instalation on your local
machine and check if they don't contain syntax errors and return valid data.
Fake-Pydoop can read text or hadoop sequence file as an input and print
calculated data on stdout or to given output file.

$ ./fake-pydoop.py your-pydoop-task.py

For example wordcount example from pydoop:

$ ./fake-pydoop.py /path/to/pydoop/examples/wordcount/bin/wordcount-minimal.py < /path/to/pydoop/examples/input/alice.txt

Based on Pure Python SequenceFile Reader/Writer work of Matteo Bertozzi
<theo.bertozzi@gmail.com> and others.
(see: https://github.com/matteobertozzi/Hadoop/tree/master/python-hadoop)
