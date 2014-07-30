# -*- coding: UTF-8 -*-
import os
import uuid
import math
import time
import socket
import logging
import pickle

class Client(object):
	"""docstring for Client"""
	def __init__(self, namenode):
		self.namenode = namenode

	def write(self, filename, data):
		chunks = [] #���data�ֳ�����num_chunks������
		chunkloc = 1
		num_chunks = self.get_num_chunks(data)
		for i in range(0, len(data), self.namenode.chunksize):
			chunks.append(data[i:i+self.namenode.chunksize])
		chunk_uuids = self.namenode.alloc(filename, num_chunks) #Ϊ�ļ�����ռ䣬����Ԫ����	
		for i in range(0, len(chunk_uuids)):			
			chunkloc = i % self.namenode.num_datanodes + 1
			self.namenode.datanodes[chunkloc].write(chunk_uuids[i], chunks[i]) 	
			#���ݵڶ���		
			chunkloc = chunkloc % self.namenode.num_datanodes + 1
			self.namenode.datanodes[chunkloc].write(chunk_uuids[i], chunks[i]) 

	def read(self, filename):
		if True == self.namenode.exits(filename) :
			data = ''
			chunk_uuids = self.namenode.filetable[filename]
			for chunk_uuid in chunk_uuids:
				chunkloc = self.namenode.chunktable[chunk_uuid] #��ȡuuid��DataNode��λ��
				data_temp = self.namenode.datanodes[chunkloc].read(chunk_uuid)	
				if str(-1) == data_temp: #��ȡ��ǰDataNode�ϵ�chunk�����ڣ�����ĳһ��DataNode���𻵣�
					data_temp = self.namenode.datanodes[chunkloc%self.namenode.num_datanodes + 1].read(chunk_uuid)  #����һ����ȡ
					print 'Current chunk is broken.'  #��ȡ��һ��DataNode��chunk
				data = data + data_temp
			return data
		else :
			print "The file: \"" + filename + "\" is not exits."		

	def delete(self, filename):  #ɾ���ļ�������ɾ����Ԫ����ɾ��
		if True == self.namenode.exits(filename) :
			chunk_uuids = self.namenode.filetable[filename]
			for chunk_uuid in chunk_uuids :
				chunkloc = self.namenode.chunktable[chunk_uuid]
				self.namenode.datanodes[chunkloc].delete(chunk_uuid)  #����ɾ��:��һ��
				self.namenode.datanodes[chunkloc%self.namenode.num_datanodes + 1].delete(chunk_uuid)  #����ɾ�����ڶ���
			self.namenode.delete(filename) #�߼�ɾ������Ԫ����ɾ����Ϣ
		else :
			print "The file: \"" + filename + "\" is not exits."

	def list_files(self):
		print "Files:"
		for (k, v) in self.namenode.filetable.items():
			print k

	def get_num_chunks(self, data):
		return int(math.ceil(len(data)*1.0 / self.namenode.chunksize))

class Namenode(object):
	"""docstring for Namenode"""
	def __init__(self):
		self.num_datanodes = 3
		self.chunksize = 10
		self.filetable = {}
		self.chunktable = {}
		self.datanodes = {}
		self.init_datanodes() #��ʼ����loc<-->server

	def init_datanodes(self):
		for i in range(1, self.num_datanodes+1):
			self.datanodes[i] = Datanode(i)

	def alloc(self, filename, num_chunks): #���ӳ�䣺filetable��chunktable
		chunkloc = 1
		chunk_uuids = []
		for i in range(0, num_chunks):
			chunk_uuid = uuid.uuid1();
			chunk_uuids.append(chunk_uuid)
			self.chunktable[chunk_uuid] = chunkloc
			chunkloc = chunkloc % self.num_datanodes + 1 #����ע�⣺Ҫ+1������chunklocֵ����仵
		self.filetable[filename] = chunk_uuids
		print self.filetable
		return chunk_uuids

	def delete(self, filename):
		chunk_uuids = self.filetable[filename]
		for chunk_uuid in chunk_uuids:
			self.chunktable.pop(chunk_uuid)
		self.filetable.pop(filename)

	def exits(self, filename): #����ļ��Ƿ����
		if filename in self.filetable:
			return True
		else: 
			return False
		
class Datanode(object):
	"""docstring for Datanode"""
	def __init__(self, chunkloc):
		self.chunkloc = chunkloc
		self.port = 12345 + chunkloc #ToDo��ͨ��chunk_loc ��Ӳ�ͬ�Ķ˿�
		self.address = ('127.0.0.1', self.port)

	def write(self, chunk_uuid, chunk):#д�뵽chunk
		try :
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			s.connect(self.address)
		except socket.error as msg:
			logging.error(msg)	
		data = ['write', str(chunk_uuid), chunk] #д����/�ļ���/�ļ�����
		pickle_data = pickle.dumps(data)
		s.send(pickle_data) #����д����
		s.close()

	def read(self, chunk_uuid): #��chunk��ȡ
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect(self.address)
		data = ['read', str(chunk_uuid)] #��ȡ����/�ļ���
		pickle_data = pickle.dumps(data)
		s.send(pickle_data) #����д����
		chunk = s.recv(1024) #��������
		s.close()	
		return chunk

	def delete(self, chunk_uuid):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect(self.address)
		data = ['delete', str(chunk_uuid)] #ɾ������/�ļ���
		pickle_data = pickle.dumps(data)
		s.send(pickle_data) #����д����
		print s.recv(1024) #ȷ��ɾ��--����Ҫȥ���������
		s.close()	

	def kill(self):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect(self.address)
		data = ['exit'] #�˳�����
		pickle_data = pickle.dumps(data)
		s.send(pickle_data) 
		print s.recv(1024) 
		s.close()	

class Command(object):
	"""docstring for Command"""
	def __init__(self, client):
		self.client = client
	
	def command_line(self):
		while True:
			cmd = raw_input('Input your command:\n')	
			if('upload' == cmd):
				self.upload_cmd()
			elif('download' == cmd):
				self.download_cmd()
			elif('delete' == cmd):
				filename = raw_input('Input the filename which you want to delete in HDFS:\n')
				self.client.delete(filename)
			elif('ls' == cmd):
				self.client.list_files()
			elif('exits' == cmd):
				break
			else:
				print "Wrong command. \n"

	def upload_cmd(self):
		filename = raw_input('Input the filename which you want to upload in local:\n')
		try :
			with open(filename, "r") as fr: #��ȡ�����ļ�
				data = fr.read()
				self.client.write(filename, data)	  #д��HDFS
		except IOError, e :
			print logging.error(e)

	def download_cmd(self):
		filename = raw_input('Input the filename which you want to download in HDFS:\n')
		data = self.client.read(filename)  #��ȡHDFS�ļ�
		print data 
		with open(filename, "w") as fw:  
			fw.write(data)	  #д�뱾��

def main():		
	nd = Namenode()
	client = Client(nd)	
	command = Command(client)
	command.command_line()

if __name__ == '__main__':
	main()
	# test net-DataNode
	# dn = Datanode(3)
	# dn.write('jyc1', 'hello jyc1. hello jyc1. hello jyc1.')
	# print dn.read('jyc1')
	# time.sleep(2)
	# dn.delete('jyc1')
	# dn.kill()

	# kill all
	# dn = Datanode(1)
	# dn.kill()
	# dn = Datanode(2)
	# dn.kill()
	# dn = Datanode(3)
	# dn.kill()