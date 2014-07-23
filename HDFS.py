# -*- coding: UTF-8 -*-
import os
import uuid
import math
import time

class Client(object):
	"""docstring for Client"""
	def __init__(self, namenode):
		self.namenode = namenode

	def write(self, filename, data):
		chunks = [] #存放data分出来的num_chunks份数据
		chunkloc = 1
		num_chunks = self.get_num_chunks(data)
		for i in range(0, len(data), self.namenode.chunksize):
			chunks.append(data[i:i+self.namenode.chunksize])
		chunk_uuids = self.namenode.alloc(filename, num_chunks) #为文件分配空间，完成元数据	
		for i in range(0, len(chunk_uuids)):			
			chunkloc = i % self.namenode.num_datanodes + 1
			self.namenode.datanodes[chunkloc].write(chunk_uuids[i], chunks[i]) 	
			#备份第二份		
			chunkloc = chunkloc % self.namenode.num_datanodes + 1
			self.namenode.datanodes[chunkloc].write(chunk_uuids[i], chunks[i]) 

	def read(self, filename):
		if True == self.namenode.exits(filename) :
			data = ''
			chunk_uuids = self.namenode.filetable[filename]
			for chunk_uuid in chunk_uuids:
				chunkloc = self.namenode.chunktable[chunk_uuid] #获取uuid的DataNode的位置
				data_temp = self.namenode.datanodes[chunkloc].read(chunk_uuid)	
				if -1 == data_temp: #读取当前DataNode上的chunk不存在（即：某一个DataNode被损坏）
					data_temp = self.namenode.datanodes[chunkloc%self.namenode.num_datanodes + 1].read(chunk_uuid)
					print 'Current chunk is broken.'  #读取下一个DataNode的chunk
				data = data + data_temp
			return data
		else :
			print "The file: \"" + filename + "\" is not exits."		

	def delete(self, filename):  #删除文件：物理删除和元数据删除
		if True == self.namenode.exits(filename) :
			chunk_uuids = self.namenode.filetable[filename]
			for chunk_uuid in chunk_uuids :
				chunkloc = self.namenode.chunktable[chunk_uuid]
				self.namenode.datanodes[chunkloc].delete(chunk_uuid)  #物理删除:第一份
				self.namenode.datanodes[chunkloc%self.namenode.num_datanodes + 1].delete(chunk_uuid)  #物理删除：第二份
			self.namenode.delete(filename) #逻辑删除：在元数据删除信息
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
		self.init_datanodes() #初始化：loc<-->server

	def init_datanodes(self):
		for i in range(1, self.num_datanodes+1):
			self.datanodes[i] = Datanode(i)

	def alloc(self, filename, num_chunks): #完成映射：filetable和chunktable
		chunkloc = 1
		chunk_uuids = []
		for i in range(0, num_chunks):
			chunk_uuid = uuid.uuid1();
			chunk_uuids.append(chunk_uuid)
			self.chunktable[chunk_uuid] = chunkloc
			chunkloc = chunkloc % self.num_datanodes + 1 #！！注意：要+1，否则chunkloc值不会变坏
		self.filetable[filename] = chunk_uuids
		print self.filetable
		return chunk_uuids

	def delete(self, filename):
		chunk_uuids = self.filetable[filename]
		for chunk_uuid in chunk_uuids:
			self.chunktable.pop(chunk_uuid)
		self.filetable.pop(filename)

	def exits(self, filename): #检测文件是否存在
		if filename in self.filetable:
			return True
		else: 
			return False
		
class Datanode(object):
	"""docstring for Datanode"""
	def __init__(self, chunkloc):
		self.chunkloc = chunkloc
		self.local_fs_root = "D:/HDFSTemp/Datanode" + str(chunkloc) #用不同的目录来模仿不同的Datanode
		if not os.path.isdir(self.local_fs_root):
			os.makedirs(self.local_fs_root)

	def write(self, chunk_uuid, chunk):#写入到chunk
		try:
			with open(self.local_fs_root + "/" + str(chunk_uuid), "w") as fw:
				fw.write(chunk)
		except IOError :
			print "The HDFS is broken."
	def read(self, chunk_uuid): #从chunk读取
		data = None
		try :
			with open(self.local_fs_root + "/" + str(chunk_uuid), "r") as fr:
				data = fr.read()
			return data
		except IOError :
			return -1

	def delete(self, chunk_uuid):
		try:
			os.remove(self.local_fs_root + "/" + str(chunk_uuid))
		except WindowsError:
			print "Filename:" + self.local_fs_root + "/" + str(chunk_uuid) + 'dose not exits.'

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
			with open(filename, "r") as fr: #读取本地文件
				data = fr.read()
				self.client.write(filename, data)	  #写入HDFS
		except IOError :
			print "No such file in local."

	def download_cmd(self):
		filename = raw_input('Input the filename which you want to download in HDFS:\n')
		data = self.client.read(filename)  #读取HDFS文件
		print data 
		with open(filename, "w") as fw:  
			fw.write(data)	  #写入本地

def main():		
	nd = Namenode()
	client = Client(nd)	
	command = Command(client)
	command.command_line()

if __name__ == '__main__':
	main()