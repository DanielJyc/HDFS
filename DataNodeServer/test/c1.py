# -*- coding: UTF-8 -*-
import socket
import struct
import pickle

HOST, PORT = '127.0.0.1', 9991
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((HOST, PORT))

# a_list = ['write', 'jyc', 'Hello jyc.' ]
# data = pickle.dumps(a_list)
# sock.send(data)

# a_list = ['read', 'jyc' ]
# data = pickle.dumps(a_list)
# sock.send(data)
# print sock.recv(1024)

# a_list = ['delete', 'jyc']
# data = pickle.dumps(a_list)
# sock.send(data)
# print sock.recv(1024)

a_list = ['exit', 'jyc']
data = pickle.dumps(a_list)
sock.send(data)
print sock.recv(1024)

sock.close()