# -*- coding: UTF-8 -*-
import socket
import struct
import pickle

HOST, PORT = '127.0.0.1', 9997
a_list = [str(x) for x in range(50) ]
data = pickle.dumps(a_list)
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect((HOST, PORT))
sock.send(data)
sock.close()