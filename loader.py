from __future__ import print_function, absolute_import, unicode_literals, division
from stackable.stack import Stack
from stackable.utils import StackablePickler
from stackable.network import StackableSocket, StackablePacketAssembler
from sys import modules
from types import ModuleType
from threading import Lock

class DispatchLoader(object):
	def __init__(self, ip, port, job):
		self.stack = Stack((StackableSocket(ip=ip, port=port),
				              StackablePacketAssembler(),
				              StackablePickler()))
		self.job = job
		self.cache = {}
		self.import_lock = Lock()

	def get_module(self, name):
		try:
			return self.cache[name][0]
		except:
			with self.import_lock:
				self.stack.write({'load': name, 'id': self.job})
				o = self.stack.read()
				if o['module'] != None:
					self.cache[name] = o['module']
					return self.cache[name][0]

	def find_module(self, fullname, path=None):
		if self.get_module(fullname) != None:
			self.path = path
			return self
		return None

	def load_module(self, name):
		if name in modules:
			return modules[name]

		m = ModuleType(name, name)
		modules[name] = m
		mod = self.get_module(name)
		if mod == None:
			raise ImportError("No such module")
		exec mod in m.__dict__
		return m
