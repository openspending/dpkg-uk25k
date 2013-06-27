import unittest
import os
from cStringIO import StringIO

import spend_diff

test_sample_dir = os.path.join(os.path.dirname(__file__), 'test_samples')

class TestBasic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.lines = list(spend_diff.spend_diff(os.path.join(test_sample_dir, 'basic1.csv'),
                                               os.path.join(test_sample_dir, 'basic2.csv'),
                                               'RowID'))

    def test_header(self):
        self.assertEqual(self.lines[0], 'RowID,Description')
    def test_changed(self):
        self.assertIn('5,Something changed', self.lines)
    def test_new(self):
        self.assertIn('6,Something new', self.lines)
    def test_unchanged(self):
        self.assertNotIn('1,Something unmoved 1', self.lines)
    def test_swapped(self):
        self.assertNotIn('2,Something swapped 2', self.lines)
        self.assertNotIn('3,Something swapped 3', self.lines)
    def test_blank_diff(self):
        # identical files yields nothing (no header row even)
        # so that it is clear there is nothing to load into
        # OpenSpending
        basic1 = os.path.join(test_sample_dir, 'basic1.csv')
        self.assertEqual(list(spend_diff.spend_diff(basic1, basic1,
                                                    'RowID')),
                         [])


class TestDifficult(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.lines = list(spend_diff.spend_diff(os.path.join(test_sample_dir, 'difficult1.csv'),
                                               os.path.join(test_sample_dir, 'difficult2.csv'),
                                               'RowID'))

    def test_header(self):
        self.assertEqual(self.lines[0], 'RowID,Description,Empty')
    def test_changed(self):
        self.assertIn('1,"first,one",', self.lines)
        self.assertIn("2,'second,one',", self.lines)
        self.assertIn('3,"blank\nline",empty', self.lines)

class TestUtil(unittest.TestCase):
    def test_parse_csv(self):
        self.assertEqual(spend_diff.parse_csv_line('1,2,3'), ['1', '2', '3'])
        self.assertEqual(spend_diff.parse_csv_line('1,"2",3'), ['1', '2', '3'])
        self.assertEqual(spend_diff.parse_csv_line('1,"2,",3'), ['1', '2,', '3'])
        self.assertEqual(spend_diff.parse_csv_line('1,"2,3",4'), ['1', '2,3', '4'])
        self.assertEqual(spend_diff.parse_csv_line('1,",3",4'), ['1', ',3', '4'])
        self.assertEqual(spend_diff.parse_csv_line('1,2,,3'), ['1', '2', '', '3'])
        self.assertEqual(spend_diff.parse_csv_line('1,2,3,'), ['1', '2', '3', ''])
        self.assertEqual(spend_diff.parse_csv_line(',,,'), ['', '', '', ''])

    def test_csv_rows(self):
        self.assertEqual(list(spend_diff.csv_rows(StringIO('1,1\n2,2'))),
                         [('1,1', ['1', '1']),
                          ('2,2', ['2', '2'])])
        self.assertEqual(list(spend_diff.csv_rows(StringIO('"1\n1",1\n2,2'))),
                         [('"1\n1",1', ['1\n1', '1']),
                          ('2,2', ['2', '2'])])

if __name__ == '__main__':
    unittest.main()
