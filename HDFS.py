import os

class Datanode(object):
	"""docstring for Datanode"""
	def __init__(self, chunkloc):
		super(Datanode, self).__init__()
		self.chunkloc = chunkloc
		self.local_fs_root = "D:/HDFSTemp/Datanode" + str(chunkloc)
		if not os.path.isdir(self.local_fs_root):
			os.makedirs(self.local_fs_root)

	def write(self, chunk_uuid, chunk):
		with open(self.local_fs_root + "/" + str(chunk_uuid), "w") as fw:
			fw.write(chunk)
	
	def read(self, chunk_uuid):
		data = None
		with open(self.local_fs_root + "/" + str(chunk_uuid), "r") as fr:
			data = fr.read()
		return data

def main():
	# test
	i = 2
	t = Datanode(i)
	t.write(1234, "Hello world.")
	print t.read(1234)


if __name__ == '__main__':
	main()


		
		