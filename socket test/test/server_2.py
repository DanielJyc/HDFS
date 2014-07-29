# -*- coding: UTF-8 -*-
import socket

def hdfs_server():
	address = ('127.0.0.2', 31514)
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.bind(address)
	s.listen(5)
	while True:
		ss, addr = s.accept()
		print 'got connected from', addr
		recv_data = ss.recv(512)
		# print recv_data	
		if 'write' == recv_data :
			print 'write'
		elif 'read' == recv_data:
			print 'read'
			ss.send('This is read data.')
		elif 'delete' == recv_data:
			print 'delete'

	ss.close()
	s.close()

if __name__ == '__main__':
	hdfs_server()
