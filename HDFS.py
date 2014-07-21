# -*- coding: UTF-8 -*-
import os
import uuid

class Namenode(object):
	"""docstring for Namenode"""
	def __init__(self):
		self.num_datanodes = 3
		self.filetable = {}
		self.chunktable = {}
		self.datanodes = {}

	def init_datanodes():
		for 

	def alloc(self, filename, num_chunks):
		chunkloc = 1
		chunk_uuids = []
		for i in range(0, num_chunks):
			chunk_uuid = uuid.uuid1();
			chunk_uuids.append(chunk_uuid)
			self.chunktable[chunk_uuid] = chunkloc
			chunkloc = chunkloc % self.num_datanodes
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
	#test for Namenode
	nd = Namenode()
	print len(nd.alloc("jyc.txt", 15))


	# test for Datanode
	# i = 2
	# t = Datanode(i)
	# t.write(1234, "Hello world.")
	# print t.read(1234)




if __name__ == '__main__':
	main()


		
		