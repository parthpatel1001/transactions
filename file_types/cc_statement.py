import re

def get_row(line):
	# TODO - make date normalizing configurable
	PATTERN = re.compile(r'''((?:[^,"']|"[^"]*"|'[^']*')+)''')
	items = PATTERN.split(line.rstrip('\n').rstrip('\r').strip())[1::2]
	return [re.sub( '\s+', ' ', item).strip().replace(' ','_').replace('/','-').replace('$','').replace('"','').replace(',','') for item in items]