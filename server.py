from __future__ import print_function, absolute_import, unicode_literals, division
from messages import DispatchInquiry, Dispatched

from stackable.stackable import StackableError
from stackable.network import StackableSocket, StackablePacketAssembler
from stackable.utils import StackablePickler
from stackable.stack import Stack

from runnable.runnable import ExecRunnable
from runnable.network import RunnableServer, RequestObject

clients = []
class Dispatcher(RequestObject):
	def init(self):
		self.stack = Stack((StackableSocket(sock=self.conn),
		                   StackablePacketAssembler(),
		                   StackablePickler()))
		clients.append(self)

		self.dispatch(DispatchInquiry(msg='dispatch', payload=ExecRunnable('print "WEEEE"; import sys; print sys.path')))

	def destroy(self):
		clients.remove(self)
		self.stack.close()
		del self.stack

	def dispatch(self, w):
		self.stack.write(w)

	def receive(self):
		try:
			obj = self.stack.poll()
			if obj != None:
				if isinstance(obj,Dispatched):
					print('[RETVAL]', obj.output)
				else:
					print("[ERROR]", type(obj))
			return True
		except StackableError:
			return False

if __name__ == '__main__':
	server = RunnableServer({'reqObj': Dispatcher, 'port': 2501})
	server.execute()
