import sys


def check_py3():
    if sys.version_info.major != 3:
        raise Exception("Requires Python 3 to run properly")
