import sys
import os

context_module = os.path.abspath(__file__)
context_module_dir = os.path.dirname(context_module)

prjsqlite3_dir = os.path.realpath(os.path.join(context_module_dir, "..", "prjsqlite3"))
sys.path.append(prjsqlite3_dir)