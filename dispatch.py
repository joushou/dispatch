from __future__ import print_function, absolute_import, unicode_literals, division
from messages import DispatchInquiry, DispatchRunnable

from stackable.stackable import StackableError
from stackable.network import StackableSocket, StackablePacketAssembler
from stackable.utils import StackablePickler
from stackable.stack import Stack

from runnable.runnable import ExecRunnable
from runnable.network import RunnableServer, RequestObject

from threading import Thread
from subprocess import Popen

dispatch_clients = []

class DispatchManager(object):
	def dispatch(self, client, w):
		client.dispatch(DispatchRunnable(w))

	def retval(self, client, w):
		print('[', client, ' - RV]:', w.response)

	def spawn(self, num):
		for i in range(0, num):
			p = Popen(['pypy', 'node.py', '0.0.0.0', '2501'])

global mgr
mgr = DispatchManager()

class Dispatcher(RequestObject):
	def init(self):
		self.stack = Stack((StackableSocket(sock=self.conn),
		                   StackablePacketAssembler(),
		                   StackablePickler()))
		dispatch_clients.append(self)

		# Test thingie
		self.dispatch(DispatchRunnable(ExecRunnable('print "WEEEE"; import sys; print sys.path')))

	def destroy(self):
		dispatch_clients.remove(self)
		self.stack.close()
		del self.stack

	def dispatch(self, w):
		self.stack.write(w)

	def respond(self, o, resp):
		o.response = resp
		self.stack.write(o)

	def receive(self):
		try:
			obj = self.stack.poll()
			if obj != None:
				if isinstance(obj,DispatchRunnable):
					mgr.retval(self, obj)
				else:
					print("[ERROR]", type(obj))
			return True
		except StackableError:
			return False

if __name__ == '__main__':
	server = RunnableServer({'reqObj': Dispatcher, 'port': 2501})
	a = Thread(target=server.execute)
	a.daemon = True
	a.start()
	# Test
	print("[DISPATCHER] UP")
	mgr.spawn(4)
	a.join()
