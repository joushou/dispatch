from __future__ import print_function, absolute_import, unicode_literals, division
from runnable.runnable import ExecRunnable
from stackable.stack import Stack
from stackable.utils import StackablePickler
from stackable.network import StackableSocket, StackablePacketAssembler

from messages import DispatchInquiry, Dispatched

from pickle import dumps
from subprocess import Popen, PIPE
from threading import Thread
from sys import argv

class DispatchManager(object):
	def __init__(self):
		self.processes = []

	def monitor(self, p, d):
		def wait():
			p.wait()
			self.processes.remove(p)
			d.output = p.stdout.read()
			d.cb(d)
		Thread(target=wait).start()

	def dispatch(self, d):
		p = Popen(['pypy', '-c', 'from runnable.runnable import Runnable;from pickle import loads;from sys import argv;loads(argv[1]).execute()', dumps(d.w)], stdout=PIPE)
		self.processes.append(p)
		self.monitor(p, d)


	def killall(self):
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
		del d.cb
		print('[DISPATCHER] Job complete')
		stack.write(d)

	while True:
		o = stack.read()
		if isinstance(o, DispatchInquiry):
			if o.msg == 'dispatch':
				print('[DISPATCHER] Dispatching job')
				mgr.dispatch(Dispatched(o.payload, complete_cb))

