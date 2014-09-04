import socket
import select

class RedisPipelineException(Exception):
    pass

class AuthException(RedisPipelineException):
    def __init__(self, auth_err):
        super(AuthException, self).__init__('Authentication error: %s', auth_err)

class DisconnectedException(RedisPipelineException):
    def __init__(self, err):
        super(DisconnectedException, self).__init__('Disconencted from sever: %s', err)

class ErrorResponse(RedisPipelineException):
    def __init__(self, response):
        super(ErrorResponse, self).__init__(response)

class RedisNil(object):
    pass

class RedisParser(object):
    def __init__(self, objectCallback=None, objectCallbackPriv=None):
        # Create request/response queue
        self.responses = []
        # Set state vars
        self.inputBuf = ''
        self.handleInputState = self.waitingForReply
        self.parsedBuf = ''
        self.multiBulkCount = 0
        self.multiBulkParts = []
        self.multiBulkStack = []
        self.err = ''
        self.bulkLen = 0

        # Parsed object callback
        self.objectCallback = objectCallback
        self.objectCallbackPriv = objectCallbackPriv

    def processInput(self, data):
        self.inputBuf += data
        while self.inputBuf:
            if not self.handleInputState():
                return False
        return True

    def getObject(self):
        if self.responses:
            return self.responses.pop()
        else:
            return None

    def waitingForReply(self):
        replyType, self.inputBuf = self.inputBuf[0], self.inputBuf[1:]
        self.handleInputState = {
          '+': self.readingSuccess,
          '-': self.readingFailure,
          ':': self.readingInt,
          '$': self.readingBulk,
          '*': self.readingMultiBulk
        }.get(replyType)
        if not self.handleInputState:
            self.err = 'Unexpected data in reply'
            return False
        return True

    def readingSuccess(self):
        while self.inputBuf:
            c, self.inputBuf = self.inputBuf[0], self.inputBuf[1:]
            if c == '\n':
                self.finalizeResponse(self.parsedBuf)
                return True
            if c != '\r':
                self.parsedBuf += c
        return True

    def readingFailure(self):
        while self.inputBuf:
            c, self.inputBuf = self.inputBuf[0], self.inputBuf[1:]
            if c == '\n':
                self.finalizeResponse(ErrorResponse(self.parsedBuf))
                return True
            if c != '\r':
                self.parsedBuf += c
        return True

    def readingInt(self):
        while self.inputBuf:
            c, self.inputBuf = self.inputBuf[0], self.inputBuf[1:]
            if c == '\n':
                try:
                    self.finalizeResponse(int(self.parsedBuf))
                    return True
                except ValueError:
                    self.err = 'Invalid int response'
                    return False
            if c != '\r':
                self.parsedBuf += c
        return True

    def finalizeResponse(self, response):
        if self.multiBulkCount > 0:
            self.multiBulkCount -= 1
            self.multiBulkParts.append(response)
            if self.multiBulkCount == 0:
                while self.multiBulkStack and self.multiBulkCount == 0:
                    prevMultiBulkCount, prevMultiBulkParts = self.multiBulkStack.pop()
                    prevMultiBulkParts.append(self.multiBulkParts)
                    prevMultiBulkCount -= 1
                    self.multiBulkParts = prevMultiBulkParts
                    self.multiBulkCount = prevMultiBulkCount
                if not self.multiBulkStack and self.multiBulkCount == 0:
                    self.responses.insert(0, self.multiBulkParts)
                    if self.objectCallback:
                        self.objectCallback(self.objectCallbackPriv)
        else:
            self.responses.insert(0, response)
            if self.objectCallback:
                self.objectCallback(self.objectCallbackPriv)
        self.parsedBuf = ''
        self.handleInputState = self.waitingForReply

    def readingBulk(self):
        while self.inputBuf:
            c, self.inputBuf = self.inputBuf[0], self.inputBuf[1:]
            if c == '\n':
                try:
                    self.bulkLen = int(self.parsedBuf)
                    self.parsedBuf = ''
                    if self.bulkLen < 0: # Handle redis nil bulk reply
                        self.finalizeResponse(RedisNil())
                        return True
                    self.handleInputState = self.readingBulkContent
                except ValueError:
                    self.err = 'Invalid bulk len'
                    return False
                return True
            if c != '\r':
                self.parsedBuf += c
        return True

    def readingBulkContent(self):
        data = self.inputBuf[:self.bulkLen]
        self.inputBuf = self.inputBuf[len(data):]
        self.parsedBuf += data
        self.bulkLen -= len(data)
        if self.bulkLen == 0:
            self.handleInputState = self.readingBulkTerminator
            return True
        return True

    def readingBulkTerminator(self):
        while self.inputBuf:
            c, self.inputBuf = self.inputBuf[0], self.inputBuf[1:]
            if c == '\n':
                self.finalizeResponse(self.parsedBuf)
                return True
            elif c == '\r':
                pass
            else:
                self.err = 'Invalid bulk response termination'
                return False

    def readingMultiBulk(self):
        while self.inputBuf:
            c, self.inputBuf = self.inputBuf[0], self.inputBuf[1:]
            if c == '\n':
                try:
                    multiBulkCount = int(self.parsedBuf)
                except ValueError:
                    self.err = 'Invalid multi bulk len'
                    return False
                if multiBulkCount > 0:
                    if self.multiBulkCount:
                        self.multiBulkStack.append((self.multiBulkCount, self.multiBulkParts))
                    self.multiBulkCount = multiBulkCount
                    self.multiBulkParts = []
                    self.parsedBuf = ''
                    self.handleInputState = self.waitingForReply
                elif multiBulkCount == 0:
                    self.finalizeResponse([])
                else: # Handle redis nil multi bulk reply
                    self.finalizeResponse(RedisNil())
                return True
            if c != '\r':
                self.parsedBuf += c
        return True

class RedisPipeline(object):
    def __init__(self, host='localhost', port=6379, pipelineLength=10, password=None, timeout=60):
        # Create parser object
        self.parser = RedisParser(objectCallback=self.responseCallback)

        # Empty pipeline
        self.pendingCommands = 0

        # Connect to server
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(timeout)
        self.sock.connect((host, port))
        self.err = None
        self.connected = True
        self.pipelineLength = pipelineLength

        # Authenticate if required
        if password:
            self.sendCmd('AUTH', password)
            res = self.getResponse(block=True)
            if res != 'OK':
                if isinstance(res, Exception):
                    raise res
                else:
                    raise AuthException(res)

    def responseCallback(self, unused):
        self.pendingCommands -= 1

    def readResponses(self, pendingCommandTarget):
        block = True
        if pendingCommandTarget is None:
            block = False
            pendingCommandTarget = 0
        if self.pendingCommands and not self.connected:
            raise DisconnectedException(self.err)
        while self.pendingCommands > pendingCommandTarget:
            if not block:
                rdyFds = select.select([self.sock], [], [], 0)
                if self.sock not in rdyFds[0]:
                    break
            buf = self.sock.recv(16384)
            if not buf:
                self.err = 'Socket closed'
                self.connected = False
                raise DisconnectedException(self.err)
            if not self.parser.processInput(buf):
                self.connected = False
                raise DisconnectedException(self.err)

    def sendCmd(self, *args):
        if not self.connected:
            raise DisconnectedException(self.err)

        # Read any pending responses before sending new command (block in case of full pipeline)
        self.readResponses(None if self.pendingCommands < self.pipelineLength else self.pipelineLength-1)
        # Write the command
        cmdStr = '*%d\r\n'%len(args)
        for arg in args:
            arg = str(arg)
            cmdStr += '$%d\r\n%s\r\n'%(len(arg), arg)
        self.sock.sendall(cmdStr)
        self.pendingCommands += 1

    def expireat(self, key, unixtime):
        self.sendCmd('EXPIREAT', key, unixtime)

    def set(self, key, value):
        self.sendCmd('SET', key, value)

    def hset(self, key, field, value):
        self.sendCmd('HSET', key, field, value)

    def sadd(self, key, member):
        self.sendCmd('SADD', key, member)

    def rpush(self, key, value):
        self.sendCmd('RPUSH', key, value)

    def zadd(self, key, member, score):
        self.sendCmd('ZADD', key, score, member)

    def flushdb(self):
        self.sendCmd('FLUSHDB')
        
    def get(self, key):
        self.sendCmd('GET', key)

    def getResponse(self, block=False):
        res = self.parser.getObject()
        if not res:
            if block:
                self.readResponses(self.pendingCommands - 1)
            else:
                self.readResponses(None)
            res = self.parser.getObject()
        return res

    def flushPipeline(self):
        self.readResponses(0)
        res = self.parser.responses
        self.parser.responses = []
        return res

