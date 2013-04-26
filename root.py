from __future__ import print_function, absolute_import, unicode_literals, division

def singleton(cls):
	instances = {}
	def getinstance():
		if cls not in instances:
			instances[cls] = cls()
		return instances[cls]
	return getinstance

class RootLibrary(object):
	def __init__(self):
		self.modules = {}

	def createModule(self, name, mod):
		self.modules[name] = mod

	def getModule(self, name):
		try:
			return self.modules[name]
		except KeyError:
			return None

	def checkModule(self, name):
		return name in self.modules


@singleton
class RootDispatcher(object):
	'The central controller thing'
	def __init__(self):
		self.clients = []
		self.lib = RootLibrary()

	def get(self, i):
		return self.clients[i]

	def dispatch(self, i, w):
		print('[ROOT] Order:', w)
		i.dispatch(w)

	def retrieve(self, name):
		print('[ROOT] Dispatching:', name)
		return self.lib.getModule(name)

	def check(self, name):
		print('[ROOT] Checking:', name)
		return self.lib.checkModule(name)

	def put(self, name, module):
		print('[ROOT] Inserting:', name)
		return self.lib.createModule(name, module)

	def report(self, w):
		print('[ROOT] Return:', w.resp)

	def register(self, d):
		self.clients.append(d)

	def deregister(self, d):
		self.clients.remove(d)
