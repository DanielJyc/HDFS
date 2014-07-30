# -*- coding: UTF-8 -*-

import SocketServer
import struct
import pickle
import os

class DatanodeTCPHandler(SocketServer.BaseRequestHandler):
    def handle(self):
        self.s_data = self.request.recv(1024)
        print "%s wrote:" % self.client_address[0]
        data= pickle.loads(self.s_data)
        print data
        self.handle_cmd(data)

    def handle_cmd(self, data):
        if 'write' == data[0] :  #接收到写入命令
            print 'write command' 
            chunk_uuid = data[1]  #接收要写入的文件名
            chunk = data[2] #接收要写入的文件内容
            self.write(chunk_uuid, chunk)
        elif 'read' == data[0]:
            print 'read command' 
            chunk_uuid = data[1] #接收文件名
            self.request.sendall(str(self.read(chunk_uuid)))#从文件 filename读取内容
        elif 'delete' == data[0]: #删除指定的文件
            print 'delete command'
            chunk_uuid = data[1]
            if -1 == self.delete(chunk_uuid): #删除文件filename
                self.request.sendall('Filename dose not exits.')
            else:
                self.request.sendall('Delete done.')  #删除成功
        elif 'exit' == data[0]:
            self.request.sendall('Exit successfully.')
            exit(0)

    def write(self, chunk_uuid, chunk):#写入到chunk
        try:
            with open(str(chunk_uuid), "w") as fw:
                fw.write(chunk)
        except IOError :
            print "The HDFS is broken."

    def read(self, chunk_uuid): #从chunk读取
        data = None
        try :
            with open(str(chunk_uuid), "r") as fr:
                data = fr.read()
            return data
        except IOError :
            return -1

    def delete(self, chunk_uuid):
        try:
            os.remove(str(chunk_uuid))
        except WindowsError:
            return -1 

if __name__ == "__main__":
    HOST, PORT = '127.0.0.1', 9992
    server = SocketServer.TCPServer((HOST, PORT), DatanodeTCPHandler)
    server.serve_forever()
    # server.shutdown()
    