# -*- coding: UTF-8 -*-
import os
import uuid
import math

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
		# print chunk_uuids
		# print chunks
		for i in range(0, len(chunk_uuids)):			
			chunkloc = i % self.namenode.num_datanodes + 1
			self.namenode.datanodes[chunkloc].write(chunk_uuids[i], chunks[i]) 

	def read(self, filename):
		data = ''
		chunk_uuids = self.namenode.filetable[filename]
		# print chunk_uuids
		for chunk_uuid in chunk_uuids:
			chunkloc = self.namenode.chunktable[chunk_uuid] #获取uuid的DataNode的位置
			# print chunk_uuid
			# print chunkloc			
			data = data + self.namenode.datanodes[chunkloc].read(chunk_uuid)
		return data

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
		return chunk_uuids
		
class Datanode(object):
	"""docstring for Datanode"""
	def __init__(self, chunkloc):
		self.chunkloc = chunkloc
		self.local_fs_root = "D:/HDFSTemp/Datanode" + str(chunkloc) #用不同的目录来模仿不同的Datanode
		if not os.path.isdir(self.local_fs_root):
			os.makedirs(self.local_fs_root)

	def write(self, chunk_uuid, chunk):#写入到chunk
		with open(self.local_fs_root + "/" + str(chunk_uuid), "w") as fw:
			fw.write(chunk)
	
	def read(self, chunk_uuid): #从chunk读取
		data = None
		with open(self.local_fs_root + "/" + str(chunk_uuid), "r") as fr:
			data = fr.read()
		return data

def main():	
	#2.test for Namenode
	nd = Namenode()
	# print type(nd.datanodes[1])
	# print len(nd.alloc("jyc.txt", 15))

	#3.test for Client
	c = Client(nd)
	c.write("world", "Hello world.Hello world.Hello world.Hello world.")
	print c.read("world")

	c.write("jyc", "Hello jyc.Hello jyc.Hello jyc.Hello jyc.")
	print c.read("jyc")




	# 1.test for Datanode
	# i = 2
	# t = Datanode(i)
	# t.write(1234, "Hello world.")
	# print t.read(1234)




if __name__ == '__main__':
	main()


		
		