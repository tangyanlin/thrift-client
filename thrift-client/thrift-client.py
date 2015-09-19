#encoding=utf-8

from thrift.transport import TSocket

class ThriftClient(object):

    def __init__(self, thrift_class, host, port, timeout):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.thrift_class = thrift_class

    def _connect(self):
        self.socket = TSocket.TSocket(self.host, self.port)
        self.socketTimeout(self.timeout)
        transport_factory = self.get_transport_factory()
        protocol_factory = self.get_protocol_factory() 
        self.transport = transport_factory(self.socket)
        self.protocol = protocol_factory(self.transport)
        client = self.thrift_class.Client(self.protocol)
        self.transport.open()
        return client

    def __getattr__(self, key):
        
        def f(*args, **kwargs):
            client = self._connect()
            ret = getattr(client, key)(*args, **kwargs)
            self.transport.close()
            return ret
        
        return f

    def get_transport_factory():
        from thrift.transport import TTransport
        return TTransport.TBufferedTransport

    def get_protocol_factory():
        from thrift.protocol import TBinaryProtocol
        return TBinaryProtocol.TBinaryProtocolAccelerated

