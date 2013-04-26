from __future__ import print_function, absolute_import, unicode_literals, division
from stackable.stack import Stack
from stackable.utils import StackablePickler
from stackable.network import StackableSocket, StackablePacketAssembler
from sys import modules
from types import ModuleType

class DispatchLoader(object):
	def __init__(self, ip, port):
		self.stack = Stack((StackableSocket(ip=ip, port=port),
				              StackablePacketAssembler(),
				              StackablePickler()))
		self.cache = {}

	def get_module(self, name):
		if name in self.cache:
			return self.cache[name]
		else:
			self.stack.write({'load': name})
			o = self.stack.read()
			if o['module'] != None:
				self.cache[name] = o['module']
				return o['module']

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
