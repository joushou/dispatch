from __future__ import print_function, absolute_import, unicode_literals, division
from root import RootDispatcher

from stackable.stackable import StackableError
from stackable.network import StackableSocket, StackablePacketAssembler
from stackable.utils import StackablePickler
from stackable.stack import Stack

from runnable.network import RunnableServer, RequestObject

from uuid import uuid4

root = RootDispatcher()

# root.put("welcome", "python", compile('print("Welcome")', "<root dispatcher>", mode='exec'), 0.1, "Greeting message")
root.put("welcome1", None, "echo Hello from your $SHELL", "shell", {'description': "Greeting message"})
# root.put("welcome1", "shell", "echo Hello from your $SHELL!", 0.1, "Greeting message")

class Job(object):
	def __init__(self, name, pid, _id):
		self.name = name
		self.pid = pid
		self.id = _id
		self.modules = []
		self.err = b''
		self.out = b''
		self.alive = True
		self.ret = None

class Dispatcher(RequestObject):
	'The connection objects - represents a connected dispatching node'
	def init(self):
		self.com_id = 0
		self.stack = Stack((StackableSocket(sock=self.conn),
		                    StackablePacketAssembler(),
		                    StackablePickler()))
		self.uuid = str(uuid4())
		self.jobs = []
		root.register(self)
		self.send('init', {'uuid': self.uuid})
		# self.dispatch('welcome')
		self.dispatch('welcome1')

	def destroy(self):
		root.deregister(self)
		self.stack.close()
		del self.stack

	def dispatch(self, w):
		self.send('execute', {'name': w})

	def find_job(self, _id):
		for i in self.jobs:
			if i.id == _id:
				return i

	def print_status(self, job):
		print('[JOB %s-%d] ---- STDOUT ----' % (self.uuid, job.id))
		print(job.out)
		print('[JOB %s-%d] ---- STDERR ----' % (self.uuid, job.id))
		print(job.err)
		print('[JOB %s-%d] -- MODULELIST --' % (self.uuid, job.id))
		print(', '.join(job.modules))
		print('[JOB %s-%d] Completed' % (self.uuid, job.id), job.name)

	def reply(self, cmd, args):
		self.stack.write({'cmd': cmd, 'args': args, 'com_id': self.com_id})

	def send(self, cmd, args):
		self.com_id += 1
		self.stack.write({'cmd': cmd, 'args': args, 'com_id': self.com_id})

	def handle(self, obj):
		try:
			cmd = obj['cmd']
			args = obj['args']
		except:
			print('[DISPATCHER] Unknown blob:', obj)
			return
		if cmd == 'get_module':
			try:
				mod = root.retrieve(args['name'])
				self.reply('module_update', {'module': mod})
				self.reply('return', {'status': 0, 'cmd': 'get_module'})
			except:
				self.reply('return', {'status': -1, 'cmd': 'get_module'})
		elif cmd == 'probe_module':
			try:
				mod = root.retrieve(args['name'])
				self.reply('module_probe', {'name': args['name'], 'meta': mod['meta'] if mod != None else None})
				self.reply('return', {'status': 0, 'cmd': 'probe_module'})
			except:
				self.reply('return', {'status': -1, 'cmd': 'probe_module'})
		elif cmd == 'push_module':
			try:
				root.put(args['name'],
				         args['bytecode'],
				         args['source'],
				         args['type'],
				         args['meta'])
				self.reply('return', {'status': 0, 'cmd': 'push_module'})
			except:
				self.reply('return', {'status': -1, 'cmd': 'push_module'})
		elif cmd == 'dispatch':
			try:
				mod_name = args['name']
				targets = args['targets']
				for target in targets:
					client = root.get(target)
					client.dispatch(mod_name)
					self.reply('return', {'status': 0, 'cmd': 'dispatch', 'target': target})
			except:
				self.reply('return', {'status': -1, 'cmd': 'dispatch'})
		elif cmd == 'get_status':
			try:
				client = root.get(args['target'])
				job = client.find_job(args['job_id'])
				self.reply('status_update', {'status': job.status()})
				self.reply('return', {'status': 0, 'cmd': 'get_status'})
			except:
				self.reply('return', {'status': -1, 'cmd': 'get_status'})
		elif cmd == 'get_clients':
			try:
				clients = root.get_uuids()
				self.reply('client_list', {'clients': clients})
				self.reply('return', {'status': 0, 'cmd': 'get_clients'})
			except:
				self.reply('return', {'status': -1, 'cmd': 'get_clients'})
		elif cmd == 'get_jobs':
			try:
				client = root.get(args['target'])
				jobs = [{'job_id': i.id, 'name': i.name, 'modules': i.modules, 'alive': i.alive, 'pid': i.pid} for i in client.jobs]
				self.reply('job_list', {'jobs': jobs, 'target': client.uuid})
				self.reply('return', {'status': 0, 'cmd': 'get_clients'})
			except:
				self.reply('return', {'status': -1, 'cmd': 'get_clients'})

		# Replies from dispatch
		elif cmd == 'dispatched':
			try:
				mod_name = args['name']
				job_id = args['job_id']
				pid = args['pid']
				self.jobs.append(Job(mod_name, pid, job_id))
			except:
				pass
		elif cmd == 'dispatch_failed':
			try:
				mod_name = args['name']
				print('[JOB %s-?] Dispatch of %s failed' % (self.uuid, mod_name))
			except:
				pass
		elif cmd == 'status_update':
			try:
				job = self.find_job(args['job_id'])
				if job != None:
					status = args['status']
					job.out = status[0]
					job.err = status[1]
					job.modules = status[2]
					job.alive = status[3]
					job.ret = status[4]
					if not job.alive:
						self.print_status(job)
			except:
				pass
		else:
			print('[DISPATCHER] Unknown blob:', obj)

	def receive(self):
		try:
			obj = self.stack.poll()
			if obj != None:
				self.handle(obj)
			return True
		except:
			return False

if __name__ == '__main__':
	print('[DISPATCHER] Starting dispatch server')
	server = RunnableServer(2501, Dispatcher)
	server.execute()
