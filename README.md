Fake-Pydoop
===========

Fake pydoop implementation for testing purposes. You can run your map reduce
pydoop task without hadoop and pydoop instalation on your local machine and
check if they don't contain syntax errors and return valid data. Fake-Pydoop
can read text or hadoop sequence file as an input and print calculated data on
stdout or to given output file.

$ ./fake-pydoop your-pydoop-task.py ./input.sqf

