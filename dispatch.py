from __future__ import print_function, absolute_import, unicode_literals, division
from stackable.stack import Stack
from stackable.utils import StackablePickler
from stackable.network import StackableSocket, StackablePacketAssembler
from stackable.stackable import StackableError
from runnable.network import RunnableServer, RequestObject

from subprocess import Popen, PIPE
from threading import Thread, Lock
from sys import argv
from uuid import uuid4

class Job(object):
	def __init__(self, name, p, _id):
		self.name = name
		self.p = p
		self.id = _id
		self.modules = []
		self.err = ''
		self.out = ''
		self.alive = True

	def kill(self):
		self.p.kill()
		self.alive = False

	def monitor(self, cb):
		def wait():
			self.p.wait()
			self.alive = False
			cb(self)
		Thread(target=wait).start()

	def status(self):
		self.out += self.p.stdout.read()
		self.err += self.p.stderr.read()
		return (self.out, self.err, self.modules, self.alive)

	def wait(self):
		x = self.p.wait()
		self.alive = False
		return x

class DispatchManager(object):
	def __init__(self, ip=None, port=None):
		self.processes = []
		self.plock = Lock()
		self.cache = {}
		self.mod_cbs = {}
		self.stack = None
		self.job_cnt = 0
		self.id = str(uuid4())
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

	def handle(self, obj):
		cmd = obj['cmd']
		args = obj['args']
		if cmd == 'init':
			self.id = args['uuid']
			print("[DISPATCHER] Setting UUID: %s" % self.id)
		elif cmd == 'execute':
			j = self.dispatch(args['name'], lambda d: self.send('status_update', {'job_id': d.id, 'status': d.status()}))
			self.send('dispatched', {'name': j.name, 'pid': 0, 'job_id': j.id})
		elif cmd == 'kill':
			job = self.get_job(args['job_id'])
			job.kill()
			self.send('status_update', {'job_id': job.id, 'status': job.status()})
			self.processes.remove(job)
		elif cmd == 'module_update':
			name = args['name']
			version = args['version']
			description = args['description']
			mod = args['module']
			self.cache[name] = (name, mod, version, description)
			if name in self.mod_cbs:
				for i in self.mod_cbs[name]:
					i(mod)
		elif cmd == 'get_status':
			job = self.get_job(args['job_id'])
			self.send('status_update', {'job_id': job.id, 'status': job.status()})
		elif cmd == 'get_jobs':
			jobs = [{'name': job.name, 'job_id': job.id, 'modules': job.modules} for job in self.jobs]
			self.send('job_update', {'jobs': jobs})

	def monitor(self):
		while True:
			o = self.stack.read()
			self.handle(o)

	def get_job(self, _id):
		for i in self.processes:
			if i.id == _id:
				return i

	def module(self, m, cb):
		if m in self.cache:
			cb(self.cache[m])
			return

		if m not in self.mod_cbs:
			self.mod_cbs[m] = []
		self.mod_cbs[m].append(cb)
		self.send('get_module', {'name': m})

	def dispatch(self, name, cb):
		with self.plock:
			print('[DISPATCHER] Spawning', name)
			p = Job(name, Popen(['pypy', '-c', '''
from sys import meta_path
from loader import DispatchLoader as __l
__dispatch__ = __l('%s', %s, %d)
meta_path = [__dispatch__]
del meta_path
exec __dispatch__.get_module('%s')''' % ("localhost", 2001, self.job_cnt, name)], stdout=PIPE, stderr=PIPE), self.job_cnt)
			self.job_cnt += 1

			self.processes.append(p)
			p.monitor(cb)
			return p

mgr = DispatchManager(argv[1], int(argv[2]))

class LoaderConnection(RequestObject):
	def init(self):
		self.stack = Stack((StackableSocket(sock=self.conn),
		                   StackablePacketAssembler(),
		                   StackablePickler()))

	def destroy(self):
		self.stack.close()
		del self.stack

	def receive(self):
		try:
			obj = self.stack.poll()
			if obj != None:
				try:
					print('[LOADER]', obj['load'], 'to Job', obj['id'])
					def respond(x):
						if x != None:
							mgr.get_job(obj['id']).modules.append(obj['load'])
						self.stack.write({'module': x})

					mgr.module(obj['load'], respond)
				except:
					print('[LOADER] Received malformed request:', obj)
			return True
		except StackableError:
			return False

server = RunnableServer(2001, LoaderConnection)
server_thread = Thread(target=server.execute)
server_thread.daemon = True
server_thread.start()
print("[DISPATCHER] Ready")
mgr.monitor()
