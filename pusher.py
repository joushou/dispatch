from __future__ import print_function, absolute_import, unicode_literals, division
from stackable.stack import Stack
from stackable.utils import StackablePickler
from stackable.network import StackableSocket, StackablePacketAssembler
from stackable.stackable import StackableError
from runnable.network import RunnableServer, RequestObject

from subprocess import Popen, PIPE
from threading import Thread, Lock
from sys import argv, stdout
import json

mode = 'file'
def draw_prompt():
	stdout.write('\n[%s] ' % mode)
	stdout.flush()

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

	def push_module(self, name, bytecode, source, _type, meta):
		self.send('push_module', {'name': name, 'bytecode': bytecode, 'source': source, 'type': _type, 'meta': meta})

	def dispatch(self, dispatcher, module):
		self.send('dispatch', {'name': module, 'targets': [dispatcher]})

	def status(self, dispatcher, job):
		self.send('get_status', {'target': dispatcher, 'job_id': job})

	def get_clients(self):
		self.send('get_clients', {})

	def get_jobs(self, dispatcher):
		self.send('get_jobs', {'target': dispatcher})

	def close(self):
		self.stack.close()

	def monitor(self):
		while True:
			o = self.stack.read()
			print(json.dumps(o,sort_keys=True, indent=3))
			draw_prompt()

dp = DispatchPusher(argv[1], int(argv[2]))
a = Thread(target=dp.monitor)
a.daemon = True
a.start()

while True:
	x = raw_input('[%s] ' % mode)

	if x == '':
		continue

	if x[:2] == '!!':
		mode = x[2:]
		print(' --> Changing mode to %s' % mode)
		continue

	if mode == 'file':
		x = x.partition(' ')
		_type = x[0]
		name = x[2].rpartition('/')[2].rpartition('.')[0]

		f = b''
		try:
			f = open(x[2]).read()
		except:
			print(' --> Failed to read %s' % name)
			continue

		bytecode = None
		if _type == 'python':
			try:
				bytecode = compile(f, name, mode='exec', dont_inherit=True)
			except:
				print(' --> Failed to compile %s' % name)
				continue

		print(' --> Prepared %s' % name)

		dp.push_module(name, bytecode, f, _type, {})
	elif mode == 'dispatcher':
		x = x.split(' ')
		cmd = x[0]
		if cmd == 'dispatch':
			print(' --> Dispatching', x[2], 'to', x[1])
			dp.dispatch(x[1], x[2])
		elif cmd == 'status':
			print(' --> Getting status of', x[1]+'-'+x[2])
			dp.status(x[1], x[2])
		elif cmd == 'get_clients':
			print(' --> Gettings clients')
			dp.get_clients()
		elif cmd == 'get_jobs':
			print(' --> Getting jobs from', x[1])
			dp.get_jobs(x[1])

	elif mode == 'console':
		if x == 'close':
			dp.close()
			raise KeyboardInterrupt()

