from __future__ import print_function, absolute_import, unicode_literals, division
from stackable.stack import Stack
from stackable.utils import StackablePickler
from stackable.network import StackableSocket, StackablePacketAssembler
import sys, types, threading

class DispatchLoader(object):
	def __init__(self, ip, port, job):
		self.stack = Stack((StackableSocket(ip=ip, port=port),
				              StackablePacketAssembler(),
				              StackablePickler()))
		self.job = job
		self.cache = {}
		self.path = ''
		self.import_lock = threading.Lock()

	def get_module(self, fullname):
		if fullname in self.cache:
			return self.cache[fullname]
		else:
			with self.import_lock:
				self.stack.write({'load': fullname, 'id': self.job})
				o = self.stack.read()
				mod = o['module']
				if mod != None:
					if mod['type'] != "python":
						return None

					self.cache[fullname] = mod
					return mod
		return None

	def exec_module(self, module, _globals):
		if module['bytecode']:
			exec module['bytecode'] in _globals
		else:
			exec module['source'] in _globals

	def execute(self, fullname, _globals):
		return self.exec_module(self.get_module(fullname), _globals)

	def find_module(self, fullname, path=None):
		if self.get_module(fullname) != None:
			self.path = path
			return self
		return None

	def load_module(self, fullname):
		if fullname in sys.modules:
			return sys.modules[fullname]

		mod = self.cache[fullname]
		m = sys.modules[fullname] = types.ModuleType(fullname, fullname)
		self.exec_module(mod, m.__dict__)
		return m
