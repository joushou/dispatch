from messages import DispatchInquiry
from root import RootDispatcher
from runnable.runnable import ExecRunnable
from BaseHTTPServer import BaseHTTPRequestHandler
from json import dumps,loads
from os.path import isfile

def js_reobjectifier(obj):
	# I want to clean this up, so it can do the conversion automatically...
	if type(obj) != dict: return obj
	for i in obj: obj[i] = js_reobjectifier(obj[i])
	if '_js_type' in obj:
		if obj['_js_type'] == 'DispatchInquiry': return DispatchInquiry.from_js(obj)
		elif obj['_js_type'] == 'ExecRunnable':
			return ExecRunnable(obj['source'], obj['filename'], obj['loc'], obj['glob'])
	return obj

root = RootDispatcher()

class DispatchMonitor(BaseHTTPRequestHandler):
	'The HTTP request handler'
	def do_HEAD(s):
		s.send_response(200)
		s.send_header("Content-type", "text/html")
		s.end_headers()

	def do_GET(s):
		path = s.path[1:]
		path = './'+path if path != '' else './index.html'
		if isfile(path):
			s.send_response(200)
			s.end_headers()
			with open(path) as f:
				s.wfile.write(f.read())

	def do_POST(s):
		s.send_response(200)
		s.send_header('Content-type', 'application/json')
		s.end_headers()
		if s.path == '/clients':
			s.wfile.write([str(i) for i in root.clients])
		elif s.path == '/pending':
			s.wfile.write([str(i) for i in root.pending])
		elif s.path == '/completed':
			s.wfile.write([str(i) for i in root.completed])
		elif s.path == '/dispatch':
			if 'Content-Length' in s.headers:
				d = s.rfile.read(int(s.headers['Content-Length']))
				d = loads(d)
				root.dispatch(DispatchInquiry(payload=ExecRunnable(d['content'], filename=d['name']), tgt=d['target']))
