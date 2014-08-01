# -*- coding: UTF-8 -*-

import socket

def hdfs_server():
	address = ("127.0.0.1", 31519)
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
			print ss.recv(1024)  #接收要写入的文件名
			ss.send('done') #告诉Client文件名接收完毕
			print ss.recv(1024) #接收要写入的文件内容
			#ToDo 写入本地硬盘
		elif 'read' == recv_data:
			print 'read command' 
			ss.send('done') #确认收到读命令
			filename = ss.recv(1024) #接收文件名
			data = 'This is read data.' #从文件 filename读取内容放入data
			ss.send(data)
		elif 'delete' == recv_data: #删除指定的文件
			print 'delete command'
			ss.send('done')
			filename = ss.recv(1024)
			#删除文件filename
			ss.send('Delete done.')  #删除成功
		elif 'exit' == recv_data:
			break
	ss.close()
	s.close()

if __name__ == '__main__':
	hdfs_server()
