import redispipeline
import subprocess
import tempfile
import unittest
import os
import time

REDIS_SERVER_PORT = 40201
temp_server_port = REDIS_SERVER_PORT+1

redis_conf = """
daemonize no
port %d
bind 127.0.0.1
logfile "/dev/null"
databases 16
dir /tmp
# requirepass foobared
appendonly no
"""

def start_redis_server(conf):
    with tempfile.NamedTemporaryFile(delete=False) as conffile:
        conffile.write(conf)
        confname = conffile.name
        
    server_proc = subprocess.Popen(['redis-server', confname])
    time.sleep(1)
    if not (server_proc.poll() is None):
        raise Exception('Failed starting redis server for tests')
    return (server_proc, confname)
    
def stop_redis_server(proc, conf):
    try:
        if proc.poll() is None:
            proc.terminate()
            proc.wait(5)
    except:
        pass

    try:                    
        os.delete(conf)
    except:
        pass            

class TestPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server_proc, cls.server_conf = start_redis_server(redis_conf%REDIS_SERVER_PORT)

    @classmethod
    def tearDownClass(cls):
        stop_redis_server(cls.server_proc, cls.server_conf)

    def parser_callback(self, priv):
        self.temp = priv
        
    def test_block_when_pipeline_full(self):
        r = redispipeline.RedisPipeline(host='localhost', port=REDIS_SERVER_PORT, pipelineLength=2, timeout=5)
        r.sendCmd('DEBUG', 'SLEEP', 1)
        r.sendCmd('DEBUG', 'SLEEP', 1)
        start = time.time()
        r.sendCmd('PING')
        elapsed = time.time() - start
        self.assertGreaterEqual(elapsed, 2)
        self.assertEqual(r.flushPipeline(), ['PONG', 'OK', 'OK'])

    def test_auth_when_no_password(self):
        self.assertRaises(redispipeline.ErrorResponse, redispipeline.RedisPipeline, host='localhost', port=REDIS_SERVER_PORT, pipelineLength=2, password='badger', timeout=5)
        
    def test_auth_with_wrong_pw(self):
        global temp_server_port
        pw_conf = redis_conf + '\nrequirepass secret'
        proc,conf = start_redis_server(pw_conf%temp_server_port)
        try:
            self.assertRaises(redispipeline.ErrorResponse, redispipeline.RedisPipeline, host='localhost', port=temp_server_port, pipelineLength=2, password='badger', timeout=5)
        finally:            
            stop_redis_server(proc,conf)
            temp_server_port += 1 # Avoid reuse addr issues when we need another temp server

    def test_auth(self):
        global temp_server_port
        pw_conf = redis_conf + '\nrequirepass secret'
        proc,conf = start_redis_server(pw_conf%temp_server_port)
        try:
            r = redispipeline.RedisPipeline(host='localhost', port=temp_server_port, pipelineLength=2, password='secret', timeout=5)
            self.assertIsNot(r, None)
            r.sendCmd('PING')
            self.assertEqual(r.getResponse(block=True), 'PONG')
        finally:            
            stop_redis_server(proc,conf)
            temp_server_port += 1 # Avoid reuse addr issues when we need another temp server
            
    def test_set(self):
        r = redispipeline.RedisPipeline(port=REDIS_SERVER_PORT)
        r.set('x', 'set_test')
        self.assertEqual(r.getResponse(block=True), 'OK')

    def test_get(self):
        r = redispipeline.RedisPipeline(port=REDIS_SERVER_PORT)
        r.set('x', 'get_test')
        r.flushPipeline()        
        r.get('x')
        self.assertEqual(r.getResponse(block=True), 'get_test')
        
    # TODO: lots of other tests!!        


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestPipeline)
    
