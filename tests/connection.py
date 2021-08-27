import unittest
import dolphindb as ddb


class MainTest(unittest.TestCase):
    def test_connect(self):
        sess = ddb.session()
        sess.connect('localhost', 9921, 'admin', '123456')
        self.assertEqual(sess.run('1 + 1'), 2)

    def test_2(self):
        sess = ddb.session()
        sess.connect('localhost', 9921, 'admin', '123456')
        array = {1.5, 2.5, 7}
        r = sess.run("sum", array)
        print(r)

if __name__ == '__main__':
    unittest.main()
