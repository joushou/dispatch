from __future__ import print_function, absolute_import, unicode_literals, division
from new import code

class Wrapper(object):
	def __init__(self, handler, script):
		self.handler = handler
		self.script = script

	def replace_consts(self, c, consts):
		new_consts = []
		for i in c.co_consts:
			for y in consts:
				if i == y:
					new_consts.append(consts[y])
					break
			else:
				new_consts.append(i)
		return code(c.co_argcount, c.co_nlocals, c.co_stacksize,
		            c.co_flags, c.co_code, tuple(new_consts), c.co_names,
		            c.co_varnames, c.co_filename, c.co_name, c.co_firstlineno,
		            c.co_lnotab, c.co_freevars, c.co_cellvars)

	def get_code(self):
		def compiler():
			import subprocess, tempfile
			t = tempfile.NamedTemporaryFile(delete=False)
			t.write("1")
			n = t.name
			t.close()
			subprocess.Popen(["2", n])
		return self.replace_consts(compiler.__code__, {"1": self.script, "2": self.handler})

if __name__ == '__main__':
	a = Wrapper('sh', 'echo "Weeeee"')
	exec a.get_code()
	b = Wrapper('ruby', 'print "Wee"')
	exec b.get_code()
