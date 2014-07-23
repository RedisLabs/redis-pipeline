import redispipeline
import unittest

class TestParser(unittest.TestCase):
    def setUp(self):
        pass

    def parser_callback(self, priv):
        self.temp = priv
        
    def test_callback(self):
        parser = redispipeline.RedisParser(objectCallback=self.parser_callback, objectCallbackPriv=99)
        parser.processInput('+DUMMY STATUS')
        self.temp = 0
        self.assertFalse(self.temp == 99)
        parser.processInput('\r\n')
        self.assertTrue(self.temp == 99)

    def test_parse_status(self):
        parser = redispipeline.RedisParser()
        parser.processInput('+DUMMY STATUS\r\n')
        self.assertEqual(parser.getObject(), 'DUMMY STATUS')
        
    def test_nested_multi_bulk(self):
        parser = redispipeline.RedisParser()
        parser.processInput('*2\r\n*2\r\n$6\r\nbadger\r\n:99\r\n*0\r\n')
        self.assertEqual(parser.getObject(), [['badger',99],[]])
        
    # TODO: lots of other tests!!        


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestParser)
    
