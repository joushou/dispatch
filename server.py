from __future__ import print_function, absolute_import, unicode_literals, division
from root import RootDispatcher

from stackable.stackable import StackableError
from stackable.network import StackableSocket, StackablePacketAssembler
from stackable.utils import StackablePickler
from stackable.stack import Stack

from runnable.network import RunnableServer, RequestObject

root = RootDispatcher()

root.put("welcome", compile('print("Welcome")', "<root dispatcher>", mode='exec'))

class Dispatcher(RequestObject):
	'The connection objects - represents a connected dispatching node'
	def init(self):
		self.stack = Stack((StackableSocket(sock=self.conn),
		                   StackablePacketAssembler(),
		                   StackablePickler()))
		root.register(self)

	def destroy(self):
		root.deregister(self)
		self.stack.close()
		del self.stack

	def dispatch(self, w):
		self.stack.write({'cmd': 'execute', 'mod_name': w})

	def receive(self):
		try:
			obj = self.stack.poll()
			if obj != None:
				if 'req' in obj:
					if obj['req'] == 'module':
						self.stack.write({'cmd': 'module', 'args': {'name': obj['args'], 'module': root.retrieve(obj['args'])}})
					elif obj['req'] == 'probe_module':
						self.stack.write({'cmd': 'module_probe', 'args': {'name': obj['args'], 'module': root.check(obj['args'])}})
				else:
					print('[DISPATCHER] Unknown blob:', obj)
			return True
		except StackableError:
			return False

	def __repr__(self):
		return "<Dispatcher object at "+str(hex(id(self)))+">"

if __name__ == '__main__':
	print('[DISPATCHER] Starting dispatch server')
	server = RunnableServer(2501, Dispatcher)
	server.execute()
