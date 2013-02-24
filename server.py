from __future__ import print_function, absolute_import, unicode_literals, division
from messages import DispatchInquiry, Dispatched

from stackable.stackable import StackableError
from stackable.network import StackableSocket, StackablePacketAssembler
from stackable.utils import StackablePickler
from stackable.stack import Stack

from runnable.runnable import ExecRunnable
from runnable.network import RunnableServer, RequestObject

from threading import Thread
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from json import dumps

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

class DispatchMonitor(BaseHTTPRequestHandler):
	def do_HEAD(s):
		s.send_response(200)
		s.send_header("Content-type", "text/html")
		s.end_headers()

	def do_GET(s):
		"""Respond to a GET request."""
		if s.path == '/':
			s.send_response(200)
			s.send_header("Content-type", "text/html")
			s.end_headers()
			with open('index.html') as f:
				s.wfile.write(f.read())
		elif s.path == '/clients':
			s.send_response(200)
			s.send_header("Content-Type", "application/json")
			s.end_headers()
			s.wfile.write(dumps([str(i) for i in clients]))

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
