# -*- coding: UTF-8 -*-
import socket
import os

def hdfs_server():
	address = ("127.0.0.1", 12346)
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.bind(address)
	s.listen(5)
	while True:
		ss, addr = s.accept()
		print 'got connected from', addr
		recv_data = ss.recv(512)
		if 'write' == recv_data :  #接收到写入命令
			print 'write command' 
			ss.send('conn')  #发送确认收到写命令
			chunk_uuid = ss.recv(1024)  #接收要写入的文件名
			ss.send('done') #告诉Client文件名接收完毕
			chunk = ss.recv(1024) #接收要写入的文件内容
			write(chunk_uuid, chunk)
			#ToDo 写入本地硬盘
		elif 'read' == recv_data:
			print 'read command' 
			ss.send('done') #确认收到读命令
			chunk_uuid = ss.recv(1024) #接收文件名
			data = read(chunk_uuid) #从文件 filename读取内容放入data
			ss.send(str(data))
		elif 'delete' == recv_data: #删除指定的文件
			print 'delete command'
			ss.send('done')
			chunk_uuid = ss.recv(1024)
			if -1 == delete(chunk_uuid): #删除文件filename
				ss.send('Filename dose not exits.')
			else:
				ss.send('Delete done.')  #删除成功
		elif 'exit' == recv_data:
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
