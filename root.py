from messages import DispatchInquiry, Dispatched
from runnable.runnable import ExecRunnable

def singleton(cls):
	instances = {}
	def getinstance():
		if cls not in instances:
			instances[cls] = cls()
		return instances[cls]
	return getinstance

class InquiryManager(object):
	def __init__(self):
		self.inqs = []

	def resolve(self, inq):
		x = None
		for i in self.inqs:
			if i.id == inq.id:
				x = i
				break
		else:
			self.inqs.append(inq)
			return inq

		x.resp = inq.resp
		self.inqs.remove(x)
		return x

inq_mgr = InquiryManager()

@singleton
class RootDispatcher(object):
	'The central controller thing'
	def __init__(self):
		self.clients = []
		self.pending = []
		self.completed = []

	def dispatch(self, w):
		print('[ROOT] Dispatching:', w)
		for i in self.clients:
			if str(i) == w.tgt:
				w = inq_mgr.resolve(w)
				self.pending.append(w)
				i.dispatch(w)

	def report(self, w):
		w = inq_mgr.resolve(w)
		self.completed.append(w)
		self.pending.remove(w)
		print('[ROOT] Return:', w.resp)

	def register(self, d):
		self.clients.append(d)
		self.dispatch(DispatchInquiry(payload=ExecRunnable('print "Welcome"'), tgt=str(d)))

	def deregister(self, d):
		self.clients.remove(d)
