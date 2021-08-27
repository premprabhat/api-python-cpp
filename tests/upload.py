import unittest
import dolphindb as ddb
import pandas as pd
import numpy as np


class MainTest(unittest.TestCase):
    def test_upload(self):
        sess = ddb.session()
        sess.connect('localhost', 9921, 'admin', '123456')
        df = pd.DataFrame({'id': np.int32([1, 2, 3, 4, 3]), 'value':  np.double([7.8, 4.6, 5.1, 9.6, 0.1]), 'x': np.int32([5, 4, 3, 2, 1])})
        sess.upload({'t1': df})

    def test_upload1(self):
        sess = ddb.session()
        sess.connect('localhost', 9921, 'admin', '123456')
        df = pd.read_pickle('./x.pickle')
        sess.upload({'t1': df})

if __name__ == '__main__':
    unittest.main()
