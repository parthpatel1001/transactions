import os
import sys
import importlib

class Query(object):
	@staticmethod
	def get(query, *args):
		try:
			q = getattr(importlib.import_module('queries.'+query), query[0].upper() + query[1:])
			q(args)

			path = os.path.join(sys.path[0], 'queries/'+query+'.sql')
			f = open(path)
			return f.read()
		except IOError:
			return query