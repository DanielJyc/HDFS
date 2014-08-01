# -*- coding: UTF-8 -*-
import socket

def hdfs_client_write(chunk_uuid=None, chunk=None):
	address = ('127.0.0.1', 31520)
	# address = ('127.0.0.2', 31514)
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect(address)
	s.send('write') #发送写命令
	if 'conn' == s.recv(1024): #判断是否连接上
		s.send(chunk_uuid)  #连接上后，发送文件名
		if 'done' == s.recv(1024):
			s.send(chunk)
	s.close()

def hdfs_client_read(chunk_uuid):
	address = ('127.0.0.1', 31520)
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect(address)
	s.send('read') #发送读取命令
	if 'done' == s.recv(1024):
		s.send(chunk_uuid)  #发送文件名
		print s.recv(1024) #接收打印数据
	s.close()

def hdfs_client_delete(chunk_uuid):
	address = ('127.0.0.1', 31520)
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect(address)
	s.send('delete') #发送读取命令
	if 'done' == s.recv(1024):
		s.send(chunk_uuid)  #发送文件名
		print s.recv(1024) #确认删除--后面要去掉这条语句
	s.close()

def hdfs_client_kill():
	address = ('127.0.0.1', 31520)
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	s.connect(address)
	s.send('exit') #发送读取命令
	print s.recv(1024)
	s.close()

if __name__ == '__main__':
	hdfs_client_write('jyc1', 'Hello jyc1.Hello jyc1.Hello jyc1.')
	hdfs_client_read('jyc1')
	hdfs_client_delete('jyc1')
	hdfs_client_kill()   #停止Server的运行
