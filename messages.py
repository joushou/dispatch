class Dispatched(object):
	def __init__(self, w=None, cb=None):
		self.w      = w
		self.cb     = cb
		self.retval = None
		self.output = None

class DispatchInquiry(object):
	def __init__(self, msg=None, payload=None, resp=None):
		self.msg     = msg
		self.resp    = resp
		self.payload = payload
