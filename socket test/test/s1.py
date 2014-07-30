# -*- coding: UTF-8 -*-

import SocketServer
import struct
import pickle

class DatanodeTCPHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        self.data = self.request.recv(1024)
        print "%s wrote:" % self.client_address[0]
        data= pickle.loads(self.data)
        print data
if __name__ == "__main__":
    HOST, PORT = '127.0.0.1', 9997
    server = SocketServer.TCPServer((HOST, PORT), DatanodeTCPHandler)
    server.serve_forever()
    # server.shutdown()
    