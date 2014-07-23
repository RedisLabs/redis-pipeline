import unittest
import test_pipeline
import test_parser

def suite():
    suite = unittest.TestSuite()
    suite.addTest(test_parser.suite())
    suite.addTest(test_pipeline.suite())
    return suite
