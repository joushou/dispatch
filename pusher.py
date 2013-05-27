from __future__ import print_function, absolute_import, unicode_literals, division
from stackable.stack import Stack
from stackable.utils import StackablePickler
from stackable.network import StackableSocket, StackablePacketAssembler
from stackable.stackable import StackableError
from runnable.network import RunnableServer, RequestObject

from subprocess import Popen, PIPE
from threading import Thread, Lock
from sys import argv, stdout
import shlex, json, os

def draw_prompt():
	stdout.write('\n[pusher] ')
	stdout.flush()

class DispatchPusher(object):
	def __init__(self, ip=None, port=None):
		self.stack = None
		self.com_id = 0
		self.cbs = {
			'all': [
				lambda x: print(json.dumps(x, sort_keys=True, indent=3))
			]
		}
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

	def register_cb(self, cmd, cb):
		try:
			self.cbs[cmd].append(cb)
		except:
			self.cbs[cmd] = [cb]

	def deregister_cb(self, cmd, cb):
		try:
			self.cbs[cmd].remove(cb)
		except:
			pass

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
			if o['cmd'] in self.cbs:
				for cb in self.cbs[o['cmd']]:
					cb(o['args'])

dp = DispatchPusher(argv[1], int(argv[2]))
a = Thread(target=dp.monitor)
a.daemon = True
a.start()

clients = {}

def prepare_client_stats(mgr):
	temp_clients = {}
	pending = 0
	def update_clients(o):
		global pending
		pending = len(o['clients'])
		for i in o['clients']:
			mgr.get_jobs(i)

	def update_jobs(o):
		global pending, clients
		temp_clients[o['target']] = {'jobs': o['jobs']}
		pending -= 1
		if pending == 0:
			clients = temp_clients

	mgr.register_cb("client_list", update_clients)
	mgr.register_cb("job_list", update_jobs)

prepare_client_stats(dp)

def update_client_stats(mgr):
	mgr.get_clients()

def add_file(_type, name, filename):
	f = None
	try:
		f = open(filename).read()
	except:
		print(' --> failed to read %s' % filename)
		return

	bytecode = None
	# if _type == 'python':
	# 	try:
	# 		bytecode = compile(f, name, mode='exec', dont_inherit=True)
	# 	except:
	# 		print(' --> Failed to compile %s' % name)
	# 		return

	print(' --> Prepared %s' % name)
	dp.push_module(name, bytecode, f, _type, {})

supported_filetypes = {
	'python': ['py'],
	'shell': ['sh', ''],
	'ruby': ['rb']
}

while True:
	x = raw_input('[pusher] ')
	x = shlex.split(x)

	if len(x) == 0:
		continue

	cmd = x[0]
	if cmd == 'add':
		_type = x[1]
		name = x[2]
		filename = x[3]
		if os.path.isdir(filename):
			for root, dirs, files in os.walk(filename):
				if root != '':
					fixed_root = root.replace('/', '.').replace(root.partition('/')[0], name)+'.'
				for _file in files:
					file_part = _file.partition('.')
					if file_part[2] in supported_filetypes[_type]:
						add_file(_type, fixed_root+file_part[0], os.path.join(root, _file))
		elif os.path.isfile(filename):
			add_file(_type, name, filename)
		else:
			print(' --> Path does not exist')
	elif cmd == 'dispatch':
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
	elif cmd == 'update_stats':
		print(' --> Updating client statistics')
		update_client_stats(dp)
	elif cmd == 'view_stats':
		print(' --> Displaying client statistics')
		for i in clients:
			print(i)
			alive_jobs = []
			done_jobs = []
			for y in clients[i]['jobs']:
				if y['alive']:
					alive_jobs.append(y)
				else:
					done_jobs.append(y)
			print('  Running jobs:   %d' % len(alive_jobs))
			for z in alive_jobs:
				print('    %s' % z['name'])
				print('       id:  %d' % z['job_id'])
				print('       pid: %d' % z['pid'])
				print('       modules')
				for y in z['modules']:
					print('          %s' % y)
			print('  Completed jobs: %d' % len(done_jobs))
			for z in done_jobs:
				print('    %s' % z['name'])
				print('       id:  %d' % z['job_id'])
				print('       pid: %d' % z['pid'])
				print('       ret: %d' % z['ret'])
				print('       modules')
				for y in z['modules']:
					print('          %s' % y)
	elif cmd == 'close' or cmd == 'quit' or cmd == 'exit':
		dp.close()
		break

