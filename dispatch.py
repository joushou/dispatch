from __future__ import print_function, absolute_import, unicode_literals, division
from runnable.runnable import ExecRunnable

from pickle import dumps
from subprocess import Popen, PIPE
from threading import Thread

dispatch_clients = []

class DispatchManager(object):
	def __init__(self):
		self.processes = []

	def monitor(self, p):
		def wait():
			p.wait()
			self.processes.remove(p)
			print('[RETURN]', p.stdout.read())
		Thread(target=wait).start()

	def dispatch(self, w):
		p = Popen(['pypy', '-c', 'from runnable.runnable import Runnable;from pickle import loads;from sys import stdin, argv;loads(argv[1]).execute()', dumps(w)], stdout=PIPE)
		self.processes.append(p)
		self.monitor(p)

	def retval(self, client, w):
		print('[', client, ' - RV]:', w.response)

global mgr
mgr = DispatchManager()

if __name__ == '__main__':
	print("[DISPATCHER] UP")
	mgr.dispatch(ExecRunnable('print "WEEEE"; import sys; import time; print sys.path; time.sleep(3)'))
	input()
