# -*- coding: UTF-8 -*-

import SocketServer
import struct

class MyTCPHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        self.data = self.request.recv(1024)
        print "%s wrote:" % self.client_address[0]
        res= struct.unpack('2B6H',self.data)
        print res
if __name__ == "__main__":
    HOST, PORT = '127.0.0.1', 9999
    server = SocketServer.TCPServer((HOST, PORT), MyTCPHandler)
    server.serve_forever()