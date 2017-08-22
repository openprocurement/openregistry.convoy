# -*- coding: utf-8 -*-
import unittest
from openregistry.convoy.tests import test_convoy, test_utils


def suite():
    suite = unittest.TestSuite()
    suite.addTest(test_convoy.suite())
    suite.addTest(test_utils.suite())
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
