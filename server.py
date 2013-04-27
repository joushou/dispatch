from __future__ import print_function, absolute_import, unicode_literals, division
from root import RootDispatcher

from stackable.stackable import StackableError
from stackable.network import StackableSocket, StackablePacketAssembler
from stackable.utils import StackablePickler
from stackable.stack import Stack

from runnable.network import RunnableServer, RequestObject

from uuid import uuid4

root = RootDispatcher()

root.put("welcome", compile('print("Welcome")', "<root dispatcher>", mode='exec'))

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
		self.stack = Stack((StackableSocket(sock=self.conn),
		                    StackablePacketAssembler(),
		                    StackablePickler()))
		self.uuid = str(uuid4())
		self.jobs = []
		root.register(self)
		self.dispatch('welcome')

	def destroy(self):
		root.deregister(self)
		self.stack.close()
		del self.stack

	def dispatch(self, w):
		self.stack.write({'cmd': 'execute', 'mod_name': w})

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

	def receive(self):
		try:
			obj = self.stack.poll()
			if obj != None:
				if 'req' in obj:
					if obj['req'] == 'module':
						self.stack.write({'cmd': 'module', 'args': {'name': obj['args'], 'module': root.retrieve(obj['args'])}})
					elif obj['req'] == 'probe_module':
						self.stack.write({'cmd': 'module_probe', 'args': {'name': obj['args'], 'module': root.check(obj['args'])}})
					elif obj['req'] == 'status':
						client = root.get(obj['args']['dispatcher'])
						job = client.find_job(obj['args']['job'])
						self.stack.write({'status': [job.out, job.err, job.modules], 'id': job.id})
				elif 'cmd' in obj:
					if obj['cmd'] == 'module':
						root.put(obj['args']['name'], obj['args']['module'])
					elif obj['cmd'] == 'dispatch':
						client = root.get(obj['args']['dispatcher'])
						client.dispatch(obj['args']['module'])
				elif 'dispatched' in obj:
					self.jobs.append(Job(obj['mod_name'], obj['pid'], obj['dispatched']))
				elif 'status' in obj:
					job = self.find_job(obj['id'])
					status = obj['status']
					job.out = status[0]
					job.err = status[1]
					job.modules = status[2]
					job.alive = status[3]
					if not job.alive:
						self.print_status(job)
				else:
					print('[DISPATCHER] Unknown blob:', obj)
			return True
		except StackableError:
			return False

if __name__ == '__main__':
	print('[DISPATCHER] Starting dispatch server')
	server = RunnableServer(2501, Dispatcher)
	server.execute()
