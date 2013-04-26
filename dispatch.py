from __future__ import print_function, absolute_import, unicode_literals, division
from stackable.stack import Stack
from stackable.utils import StackablePickler
from stackable.network import StackableSocket, StackablePacketAssembler
from stackable.stackable import StackableError
from runnable.network import RunnableServer, RequestObject

from subprocess import Popen, PIPE
from threading import Thread, Lock
from sys import argv

class Job(object):
	def __init__(self, name, p):
		self.name = name
		self.p = p
		self.alive = True

	def kill(self):
		self.p.kill()
		self.alive = False

	def stdout(self):
		return self.p.stdout.read()

	def stderr(self):
		return self.p.stderr.read()

	def monitor(self, cb):
		def wait():
			self.p.wait()
			self.alive = False
			cb(self)
		Thread(target=wait).start()

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
		if ip != None and port != None:
			self.connect(ip, port)

	def connect(self, ip, port):
		self.stack = Stack((StackableSocket(ip=ip, port=port),
				         	  StackablePacketAssembler(),
				         	  StackablePickler()))

	def monitor(self):
		def complete_cb(d):
			print('[DISPATCHER] --- STDOUT ---')
			print(d.stdout())
			print('[DISPATCHER] --- STDERR ---')
			print(d.stderr())
			print('[DISPATCHER] Completed', d.name)

		while True:
			o = self.stack.read()
			if 'cmd' in o:
				if o['cmd'] == 'execute':
					self.dispatch(o['mod_name'], complete_cb)
				elif o['cmd'] == 'kill':
					self.kill(o['mod_name'])
				elif o['cmd'] == 'module':
					name, module = o['args']['name'], o['args']['module']
					self.cache[name] = module
					if name in self.mod_cbs:
						for i in self.mod_cbs[name]:
							i(module)


	def module(self, m, cb):
		if m in self.cache:
			cb(self.cache[m])
			return

		if m not in self.mod_cbs:
			self.mod_cbs[m] = []
		self.mod_cbs[m].append(cb)
		self.stack.write({"req": "module", "args": m})

	def resume(self):
		pass

	def kill(self, m):
		for i in self.processes:
			if i.name == m:
				i.kill()
				self.processes.remove(i)

	def dispatch(self, name, cb):
		with self.plock:
			print('[DISPATCHER] Spawning', name)
			p = Job(name, Popen(['pypy', '-c', '''
from __future__ import print_function,absolute_import,unicode_literals,division;
import sys as __sys;
from loader import DispatchLoader as __l;
__sys.meta_path = [__l("%s", %s)];
__m__=__sys.meta_path[0].get_module(__sys.argv[1]);
del __sys, __l;
exec __m__''' % ("localhost", 2001), name], stdout=PIPE, stderr=PIPE))

			self.processes.append(p)
			p.monitor(cb)

	def killall(self):
		with self.plock:
			x = self.processes
			for i in x:
				i.kill()
				self.processes.remove(i)

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
				if 'load' in obj:
					print('[LOADER]', obj['load'])
					mgr.module(obj['load'], lambda x: self.stack.write({'module': x}))
			return True
		except StackableError:
			return False

print("[DISPATCHER] Ready")


server = RunnableServer(2001, LoaderConnection)
server_thread = Thread(target=server.execute)
server_thread.daemon = True
server_thread.start()
mgr.monitor()
