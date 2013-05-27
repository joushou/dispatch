from __future__ import print_function, absolute_import, unicode_literals, division

def singleton(cls):
	instances = {}
	def getinstance():
		if cls not in instances:
			instances[cls] = cls()
		return instances[cls]
	return getinstance


@singleton
class RootDispatcher(object):
	'The central controller thing'
	def __init__(self):
		self.clients = []
		self.modules = {}

	def check_module(self, name):
		return name in self.modules

	def retrieve(self, name):
		print('[ROOT] Retrieving:', name)
		return self.modules[name]

	def put(self, name, bytecode, source, _type, meta):
		print('[ROOT] Inserting:', name)
		self.modules[name] = {
			"name": name,
			"bytecode": bytecode,
			"source": source,
			"type": _type,
			"meta": meta
		}
		return self.modules[name]

	def get(self, _id):
		for i in self.clients:
			if i.uuid == _id:
				return i

	def get_uuids(self):
		return [i.uuid for i in self.clients]

	def register(self, d):
		print('[ROOT] Registering:', d.uuid)
		self.clients.append(d)

	def deregister(self, d):
		print('[ROOT] Deregistering:', d.uuid)
		self.clients.remove(d)
