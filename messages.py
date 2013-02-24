from __future__ import print_function, absolute_import, unicode_literals, division

class DispatchMessage(object):
	def __init__(self, value=None, response=None):
		self.message = value
		self.response = response

	def __str__(self):
		return "DispatchMessage: " + repr(self.message, self.response)

class DispatchRunnable(DispatchMessage): pass
class DispatchInquiry(DispatchMessage): pass
