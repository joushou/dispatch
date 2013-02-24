from __future__ import print_function, absolute_import, unicode_literals, division
from runnable.runnable import ExecRunnable

from pickle import dumps
from subprocess import Popen, PIPE
from threading import Thread

class Dispatched(object):
	def __init__(self, w):
		self.w      = w
		self.retval = None

class DispatchManager(object):
	def __init__(self):
		self.processes = []

	def monitor(self, p, d):
		def wait():
			p.wait()
			self.processes.remove(p)
			d.response = p.stdout.read()
			self.complete(d)
		Thread(target=wait).start()

	def dispatch(self, d):
		p = Popen(['pypy', '-c', 'from runnable.runnable import Runnable;from pickle import loads;from sys import argv;loads(argv[1]).execute()', dumps(d.w)], stdout=PIPE)
		self.processes.append(p)
		self.monitor(p, d)

	def complete(self, d):
		print('['+str(d)+']:', d.response)

	def killall(self):
		x = self.processes
		for i in x:
			i.kill()
			self.processes.remove(i)

mgr = DispatchManager()

if __name__ == '__main__':
	print("[DISPATCHER] UP")
	mgr.dispatch(Dispatched(ExecRunnable('print "WEEEE"')))
	input()
