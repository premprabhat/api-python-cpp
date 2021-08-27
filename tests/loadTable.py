import unittest
import dolphindb as ddb
import pandas as pd
import numpy as np


class MainTest(unittest.TestCase):
    def test_loadTable(self):
        sess = ddb.session()
        sess.connect('localhost', 9921, 'admin', '123456')
        sess.loadTable('')


if __name__ == '__main__':
    unittest.main()
