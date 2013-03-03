from __future__ import print_function, absolute_import, unicode_literals, division
from runnable.runnable import ExecRunnable
from stackable.stack import Stack
from stackable.utils import StackablePickler
from stackable.network import StackableSocket, StackablePacketAssembler

from messages import DispatchInquiry, Dispatched, DispatchedState

from pickle import dumps
from subprocess import Popen, PIPE
from threading import Thread, Lock
from sys import argv

class DispatchManager(object):
	def __init__(self):
		self.processes = []
		self.plock = Lock()

	def monitor(self, p, d, cb):
		def wait():
			p.wait()
			res = DispatchedState(retval=None, output=p.stdout.read(), exc=None)
			with self.plock:
				self.processes.remove(p)
				d.resp = res
				cb(d)
		Thread(target=wait).start()

	def dispatch(self, d, cb):
		with self.plock:
			r = d.payload
			p = Popen(['pypy', '-c', 'from pickle import loads as __ploads;from sys import argv as __argv;__ploads(__argv[1]).execute()', dumps(r)], stdout=PIPE)
			self.processes.append(p)
			self.monitor(p, d, cb)

	def killall(self):
		with self.plock:
			x = self.processes
			for i in x:
				i.kill()
				self.processes.remove(i)

mgr = DispatchManager()

if __name__ == '__main__':
	print("[DISPATCHER] Ready")
	stack = Stack((StackableSocket(ip=argv[1], port=int(argv[2])),
	              StackablePacketAssembler(),
	              StackablePickler()))

	def complete_cb(d):
		print('[DISPATCHER] Job complete')
		stack.write(d)

	while True:
		o = stack.read()
		if isinstance(o, DispatchInquiry):
			mgr.dispatch(o, complete_cb)

