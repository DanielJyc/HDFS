# -*- coding: UTF-8 -*-
import socket
import os
import pickle

def hdfs_server():
	address = ("127.0.0.1", 12347)
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.bind(address)
	s.listen(5)
	while True:
		ss, addr = s.accept()
		print 'got connected from: ', addr
		recv_pickle_data = ss.recv(1024)
		data = pickle.loads(recv_pickle_data)
		if 'write' == data[0] :  #接收到写入命令
			print 'write command' 
			chunk_uuid = data[1]  #接收要写入的文件名
			chunk = data[2] #接收要写入的文件内容
			write(chunk_uuid, chunk)
		elif 'read' == data[0]:
			print 'read command' 
			chunk_uuid = data[1] #接收文件名
			data = read(chunk_uuid) #从文件 filename读取内容放入data
			ss.send(str(data))
		elif 'delete' == data[0]: #删除指定的文件
			print 'delete command'
			ss.send('done')
			chunk_uuid = data[1]
			if -1 == delete(chunk_uuid): #删除文件filename
				ss.send('Filename dose not exits.')
			else:
				ss.send('Delete done.')  #删除成功
		elif 'exit' == data[0]:
			ss.send('Exit successfully.')
			break
	ss.close()
	s.close()

def write(chunk_uuid, chunk):#写入到chunk
	try:
		with open(str(chunk_uuid), "w") as fw:
			fw.write(chunk)
	except IOError :
		print "The HDFS is broken."
def read(chunk_uuid): #从chunk读取
	data = None
	try :
		with open(str(chunk_uuid), "r") as fr:
			data = fr.read()
		return data
	except IOError :
		return -1

def delete( chunk_uuid):
	try:
		os.remove(str(chunk_uuid))
	except WindowsError:
		return -1 

if __name__ == '__main__':
	hdfs_server()
