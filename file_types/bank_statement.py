import re

def replace_mult(s, replacements):
	for r in replacements:
		s = s.replace(*r)
	return s

def sub_mult(s, subs):
	for r in subs:
		s = re.sub(r[0], r[1], s)
	return s

col_row = 'Transaction Date,Description,Debit'
replacements = [
	(',', ''),
	(' ', '_'),
	('/', '-'),
	('$', ''),
	('ACH',''),
	('Withdrawal', ''),
	('Deposit', ''),
	('deposit', ''),
	('PARTH',''),
	('PATEL',''),
	('0U411_A02T_CJP1',''),
	('0U411_A02T_CMA1','')
]
subs = [
	('\_\_+', ''),
	('\s+',' '),
	('\.\.+','')
]
def get_row(line):
	global replacements, subs, col_row
	# http://stackoverflow.com/questions/2785755/how-to-split-but-ignore-separators-in-quoted-strings-in-python
	PATTERN = re.compile(r'''((?:[^,"']|"[^"]*"|'[^']*')+)''')
	items = PATTERN.split(line.rstrip('\n').rstrip('\r').strip())[1::2]

	items = [sub_mult(replace_mult(item, replacements), subs).strip() for item in items]

	if col_row in line:
		items.append('Credit')
		return items

	if items[0].upper() == 'PENDING':
		return None

	amnt = float(items.pop())
	
	if amnt < 0:
		items.append(str(amnt * -1))
		items.append('')
	else:
		items.append('')
		items.append(str(amnt))
	return items
