from __future__ import print_function, absolute_import, unicode_literals, division
from root import RootDispatcher

from stackable.stackable import StackableError
from stackable.network import StackableSocket, StackablePacketAssembler
from stackable.utils import StackablePickler
from stackable.stack import Stack

from runnable.network import RunnableServer, RequestObject

from uuid import uuid4

root = RootDispatcher()

root.put("welcome", "python", compile('print("Welcome")', "<root dispatcher>", mode='exec'), 0.1, "Greeting message")

class Job(object):
	def __init__(self, name, pid, _id):
		self.name = name
		self.pid = pid
		self.id = _id
		self.modules = []
		self.err = ''
		self.out = ''
		self.alive = True

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
		self.dispatch('welcome')

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

	def report_jobs(self, cb):
		pass

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
		cmd = obj['cmd']
		args = obj['args']
		if cmd == 'get_module':
			self.reply('return', {'status': 0, 'cmd': 'get_module'})
			mod_name = args['name']
			(version, _type, description) = root.check(mod_name)
			mod = root.retrieve(mod_name)
			self.send('module_update', {'name': mod_name, 'module': mod, 'version': version, 'type': _type, 'description': description})
		elif cmd == 'probe_module':
			self.reply('return', {'status': 0, 'cmd': 'probe_module'})
			mod_name = args['name']
			(version, _type, description) = root.check(mod_name)
			self.send('module_probe', {'name': mod_name, 'version': version, 'type': _type, 'description': description})
		elif cmd == 'probe_jobs':
			self.reply('return', {'status': 0})
			pass
		elif cmd == 'push_module':
			mod_name = args['name']
			mod = args['module']
			version = args['version']
			description = args['description']
			root.put(mod_name, mod, version, description)
			self.reply('return', {'status': 0, 'cmd': 'push_module'})
		elif cmd == 'dispatch':
			mod_name = args['name']
			targets = args['targets']
			for target in targets:
				client = root.get(target)
				client.dispatch(mod_name)
				self.reply('return', {'status': 0, 'cmd': 'dispatch', 'target': target})

		# Replies from dispatch
		elif cmd == 'dispatched':
			mod_name = args['name']
			job_id = args['job_id']
			pid = args['pid']
			self.jobs.append(Job(mod_name, pid, job_id))
		elif cmd == 'status_update':
			job = self.find_job(args['job_id'])
			status = args['status']
			job.out = status[0]
			job.err = status[1]
			job.modules = status[2]
			job.alive = status[3]
			if not job.alive:
				self.print_status(job)
		else:
			print('[DISPATCHER] Unknown blob:', obj)

	def receive(self):
		try:
			obj = self.stack.poll()
			if obj != None:
				self.handle(obj)
			return True
		except StackableError:
			return False

if __name__ == '__main__':
	print('[DISPATCHER] Starting dispatch server')
	server = RunnableServer(2501, Dispatcher)
	server.execute()
