class Datanode(object):
	"""docstring for Datanode"""
	def __init__(self, arg):
		super(Datanode, self).__init__()
		self.arg = arg
	def write(self, chunk_uuid, chunk):
		with open(str(chunk_uuid), "w") as fw:
			fw.write(chunk)
	
	def read(self, chunk_uuid):
		data = None
		with open(str(chunk_uuid), "r") as fr:
			data = fr.read()
		return data

def main():
	# test
	i = 2
	t = Datanode(i)
	t.write(123, "Hello world.")
	print t.read(123)


if __name__ == '__main__':
	main()


		
		