class Dispatched(object):
	def __init__(self, w=None, _id=None):
		self.w      = w
		self.id     = _id if _id != None else id(self)

class DispatchedState(object):
	def __init__(self, retval=None, output=None, exc=None, _id=None):
		self.retval = retval
		self.output = output
		self.exc    = exc
		self.id     = _id if _id != None else id(self)

class DispatchInquiry(object):
	def __init__(self, msg=None, payload=None, tgt=None, resp=None, _id=None):
		self.id      = _id if _id != None else id(self)
		self.msg     = msg
		self.tgt     = tgt
		self.resp    = resp
		self.payload = payload
