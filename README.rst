redispipeline
=============

Non blocking pipelined python client for redis.

Usage example
-------------

.. code-block:: python

    import redispipeline
    r = redispipeline.RedisPipeline(pipeline_depth = 3)
    r.set('x',1)
    r.get('x')
    r.set('y',2)
    r.get('y') # This will block until we get the response from 'set x 1' because it's the 4th command and pipeline_depth is 3
    print r.gerResponse() # Print 'OK' (we know there's at least one response received because we blocked in previous command)
    print r.getResponse() # Print '1' (or None if no response received yet)
    r.sendCmd('PING') # I don't support all redis commands yet, but with sendCmd you can call whatever you want
    print r.flushPiepline() # Wait for all pending responses to be received, will print "['OK','2','PONG']" or "['1', 'OK','2','PONG']" depending on previous line's result

