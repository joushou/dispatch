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
		self.com_id = 0
		if ip != None and port != None:
			self.connect(ip, port)

	def connect(self, ip, port):
		self.stack = Stack((StackableSocket(ip=ip, port=port),
				         	  StackablePacketAssembler(),
				         	  StackablePickler()))

	def reply(self, cmd, args):
		self.stack.write({'cmd': cmd, 'args': args, 'com_id': self.com_id})

	def send(self, cmd, args):
		self.com_id += 1
		self.stack.write({'cmd': cmd, 'args': args, 'com_id': self.com_id})

	def push_module(self, name, module, version=0, description=''):
		self.send('push_module', {'name': name, 'module': module, 'version': version, 'description': description})

	def dispatch(self, dispatcher, module):
		self.send('dispatch', {'name': module, 'targets': [dispatcher]})

	def status(self, dispatcher, job):
		self.send('get_status', {'target': dispatcher, 'job_id': job})

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
