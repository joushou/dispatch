from __future__ import print_function, absolute_import, unicode_literals, division
from messages import DispatchInquiry, Dispatched
from web import DispatchMonitor
from root import RootDispatcher

from stackable.stackable import StackableError
from stackable.network import StackableSocket, StackablePacketAssembler
from stackable.utils import StackablePickler
from stackable.stack import Stack

from runnable.network import RunnableServer, RequestObject

from BaseHTTPServer import HTTPServer

from threading import Thread

root = RootDispatcher()

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
		self.stack.write(w)

	def receive(self):
		try:
			obj = self.stack.poll()
			if obj != None:
				if isinstance(obj,DispatchInquiry):
					root.report(obj)
				else:
					print("[ERROR]", type(obj), str(self))
			return True
		except StackableError:
			return False

	def __repr__(self):
		return "<Dispatcher object at "+str(hex(id(self)))+">"

if __name__ == '__main__':
	print('[DISPATCHER] Starting dispatch server')
	server = RunnableServer({'reqObj': Dispatcher, 'port': 2501})
	s = Thread(target=server.execute)
	s.daemon = True
	s.start()
	print('[DISPATCHER] Starting monitor')
	monitor = HTTPServer(('', 8080), DispatchMonitor)
	try:
		monitor.serve_forever()
	except KeyboardInterrupt:
		pass
	monitor.server_close()
	print('[DISPATCHER] Server shutdown complete')
