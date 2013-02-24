from __future__ import print_function, absolute_import, unicode_literals, division
from messages import DispatchInquiry, DispatchRunnable
from stackable.stackable import StackableError
from stackable.network import StackableSocket, StackablePacketAssembler
from stackable.utils import StackablePickler
from stackable.stack import Stack, StackError
from runnable.runnable import Runnable
from sys import argv

stack = Stack((StackableSocket(ip=argv[1], port=int(argv[2])),
		  StackablePacketAssembler(),
		  StackablePickler()))

print("[NODE] UP")

jobs = 0
while True:
	o = stack.read()
	if isinstance(o, DispatchRunnable):
		jobs += 1
		try:
			o.response = o.message.execute()
		except Exception as e:
			o.response = e
		stack.write(o)
	elif isinstance(o, DispatchInquiry):
		if o.message == 'shutdown':
			o.response = 'ok'
			stack.write(o)
