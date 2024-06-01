import unittest

loader = unittest.TestLoader()
suite = loader.discover(start_dir=".", pattern="test*.py")

runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)
