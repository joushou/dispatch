from __future__ import print_function, absolute_import, unicode_literals, division
from stackable.stack import Stack
from stackable.utils import StackablePickler
from stackable.network import StackableSocket, StackablePacketAssembler
from stackable.stackable import StackableError
from runnable.network import RunnableServer, RequestObject

from subprocess import Popen, PIPE
from threading import Thread, Lock
from sys import argv

class DispatchPusher(object):
	def __init__(self, ip=None, port=None):
		self.stack = None
		if ip != None and port != None:
			self.connect(ip, port)

	def connect(self, ip, port):
		self.stack = Stack((StackableSocket(ip=ip, port=port),
				         	  StackablePacketAssembler(),
				         	  StackablePickler()))

	def push_module(self, name, module):
		self.stack.write({'cmd': 'module', 'args': {'name': name, 'module': module}})

	def dispatch(self, dispatcher, module):
		self.stack.write({'cmd': 'dispatch', 'args': {'dispatcher': dispatcher, 'module': module}})

	def status(self, dispatcher, job):
		self.stack.write({'req': 'status', 'args': {'dispatcher': dispatcher, 'id': job}})

	def close(self):
		self.stack.close()

	def monitor(self):
		while True:
			o = self.stack.read()
			print(o)

dp = DispatchPusher(argv[1], int(argv[2]))
a = Thread(target=dp.monitor)
a.daemon = True
a.start()

mode = 'file'
while True:
	x = raw_input('[%s] ' % mode)

	if x == '':
		continue

	if x[:2] == '!!':
		mode = x[2:]
		print(' --> Changing mode to %s' % mode)
		continue

	if mode == 'file':
		name = x.rpartition('/')[2].partition('.py')[0]
		f = b''
		try:
			f = open(x).read()
		except:
			print(' --> Failed to read %s' % name)

		code = compile(f, name, mode='exec', dont_inherit=True)
		print(' --> Prepared %s' % name)

		dp.push_module(name, code)
	elif mode == 'dispatch':
		x = x.partition(' ')
		print(' --> Dispatching', x[2], 'to', x[0])
		dp.dispatch(x[0], x[2])
	elif mode == 'console':
		if x == 'close':
			dp.close()
			raise KeyboardInterrupt()

print("[PUSHER] Ready")
