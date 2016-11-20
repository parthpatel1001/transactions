import os
import sys
import traceback
import re
import sqlite3
import datetime
from file_types.cc_statement import get_row as cc_get_row
from file_types.bank_statement import get_row as bs_get_row
from file_types.clean_csv import get_row as clean_row
from config import Config
from queries.Query import Query

def files(path, file_type=None, ignore_file=None):
	_, _, files = next(os.walk(path))
	for file in files:
		if (file_type is not None and file_type not in file) or (ignore_file is not None and ignore_file in file):
			continue
		yield os.path.join(path, file)

get_row = {
	'cc' : cc_get_row,
	'bs' : bs_get_row,
	'clean' : clean_row
}

def get_file_type(file):
	if 'CapitalOne' in file:
		return 'bs'
	# todo clean this up	
	elif 'dedupe' in file:
		return 'clean'
	else:
		return 'cc'

def lines(file):
	row = get_row[get_file_type(file)]
	with open(file) as f:
		for line in f.readlines():
			line = row(line)
			if line is not None:
				yield line

def csv(rows, indexed_columns=True):
	if indexed_columns:
		return {col:idx for (idx,col) in enumerate(next(rows))}, rows
	else:
		return tuple(next(rows)), rows

def get(columns, row, retrieve):
	'''column index map, row of data, columns to retrieve'''
	out = []
	row_len = len(row)
	for r in retrieve:
		col_idx = columns[r] if r in columns else -1
		val = row[col_idx] if 0 <= col_idx < row_len else ''
		if 'DATE' in r.upper():
			# TODO make date format configurable
			try:
				val = datetime.datetime.strptime(val, '%m-%d-%Y').strftime('%Y-%m-%d')
			except:
				pass
		out.append(val)
	return tuple(out)

# TODO make path, file_type, ignore_file defaults configurable
# TODO add save to file option (make default save to a file, option to print)
def dedupe():
	dedupe_on, output_columns, path, file_type, ignore_file = Config.get(
		'dedupe_on', 
		'output_columns', 
		'data_dir', 
		'data_file_type', 
		'dedupe_file_name'
	)

	uniq = set()
	print ','.join(output_columns)
	for file in files(path, file_type, ignore_file):
		columns, rows = csv(lines(file))
		for row in rows:
			d = get(columns, row, dedupe_on)
			if d in uniq:
				continue
			uniq.add(d)
			print ','.join(get(columns, row, output_columns))

# TODO make dedupe file configurable
def query(query, *args):
	data_dir, deudpe_fname, f_type, table_name = Config.get(
		'data_dir',
		'dedupe_file_name',
		'data_file_type',
		'table_name'
	)
	file = data_dir + '/' + deudpe_fname + '.' + f_type

	error = None
	con = None
	query = Query.get(query, args)
	try:
		columns, rows = csv(lines(file), indexed_columns=False)
		con = sqlite3.connect(":memory:")
		cur = con.cursor()
		
		c_query = 'create table %s %s;' % (table_name, columns)
		i_query = 'insert into %s %s values (%s);' % (table_name, columns, ('?,'*len(columns))[:-1])
		i_items = [tuple(row) for row in rows]
		
		cur.execute(c_query)
		cur.executemany(i_query, i_items)
		con.commit()
		
		for row in cur.execute(query):
			print [str(i) for i in row]
	except Exception as e:
		error = e
	finally:
		if con is not None:
			con.close()
		if error is not None:
			raise error

def help(info=None):
	if info == 'query':
		global common_queries
		for (k,v) in common_queries.items():
			print k
			print '\t-', v['desc']
			# print
		return
	items = [
		'help - print options',
		'\t USAGE:',
		'\t\t python transactions.py help {info}',
		'\t\t\t {info} - help on specific menu item [queries,]',
		'dedupe - prints deduped transactions from directory containing transactions csvs',
		'\t USAGE:',
		'\t\t python transactions.py dedupe {/path/to/dir} {file_type}',
		'\t\t\t /path/to/dir - location of directory of data files, DEFAULT: /Volumes/parth_drive/transactions',
		'\t\t\t {file_type} - optional, only look at this type of file, DEFAULT: csv',

		'query - run sql queries against transactions csv',
		'\t USAGE:',
		'\t\t python transactions.py query "{query}" {file_name} {table_name}',
		'\t\t\t {query} - any valid sqlite query - NOTE: query must be in quotes',
		'\t\t\t {file_name} - transactions csv file location DEFAULT: /Volumes/parth_drive/transactions/dedupe.csv',
		'\t\t\t {table_name} - table_name to load csv into',

		'Optional:',
		'\t --debug - print traceback'
	]
	for item in items:
		print item

if __name__ == "__main__":
	try:
		if len(sys.argv) < 2:
			raise Exception('Must provide at least 1 argument')

		action = sys.argv[1]
		common_queries = {
			'*': {
				'desc' : 'get all transactions',
				'query':'select * from transactions order by transaction_date'
			},
			'cat': {
				'desc' : 'get category, count, avg by month',
				'query':"select strftime('%Y-%m',transaction_date), category, round(sum(Debit), 2), count(debit), round(sum(debit) / count(debit), 2) from transactions where credit='' group by category, strftime('%Y', Transaction_date), strftime('%m',Transaction_date) order by transaction_date"
			},
			'cat sort by cat': {
				'desc' : 'get category, count, avg by mont sorted by cat',
				'query':"select strftime('%Y-%m',transaction_date), category, round(sum(Debit), 2), count(debit), round(sum(debit) / count(debit), 2) from transactions where credit='' group by category, strftime('%Y', Transaction_date), strftime('%m',Transaction_date) order by category, transaction_date"
			},
			'catD': {
				'desc' : 'get category, count, avg by day',
				'query':"select transaction_date, Category, round(sum(Debit), 2) as total, count(debit) as num, round(sum(debit)/count(debit), 2) as avg  from transactions where credit='' group by category, transaction_date order by transaction_date"
			},
			'catY': {
				'desc' : 'get category, count, avg by year',
				'query':"select strftime('%Y', Transaction_date) as year, Category, round(sum(Debit), 2) from transactions group by category, year"
			},
			'spend': {
				'desc' : 'get total spend, count, avg by month',
				'query':"select strftime('%Y-%m',transaction_date), sum(debit), count(debit), round(sum(debit) / count(debit), 2) from transactions where cast(debit as real) > 0 group by strftime('%Y', Transaction_date), strftime('%m',Transaction_date) order by transaction_date"
			},
			'spend *': {
				'desc' : 'get all spend',
				'query':"select * from transactions where credit != '' and description not like '%PYMT%' order by transaction_date"
			},
			'spendY' : { 
				'desc'  : 'get spend by year', 
				'query' : "select strftime('%Y',transaction_date) as year, sum(debit) from transactions group by year"
			},
			'venmo':{
				'desc': 'get venmo spend by month',
				'query': "select strftime('%Y-%m',transaction_date), sum(debit), count(debit), round(sum(debit) / count(debit), 2) from transactions where category='' and description like '%venmo%payment%' group by strftime('%Y', Transaction_date), strftime('%m',Transaction_date) order by transaction_date"
			},
			'climbing': {
				'desc': 'climbing spend/mo',
				'query': "select strftime('%Y-%m',transaction_date), sum(debit), count(debit) from transactions where description like '%boulders%' or description like '%cliffs%' group by strftime('%Y', Transaction_date), strftime('%m',Transaction_date) order by transaction_date"
			},
			'climbing loc': {
				'desc': 'climbing spend/mo by location',
				'query': "select strftime('%Y-%m',transaction_date), description, count(debit), sum(debit) from transactions where description like '%boulders%' or description like '%cliffs%' group by description, strftime('%Y', Transaction_date), strftime('%m',Transaction_date) order by transaction_date"
			},
			'climbing *': {
				'desc': 'climbing spend/mo',
				'query': "select * from transactions where description like '%boulders%' or description like '%cliffs%' order by transaction_date"
			},
			'coffee *' : {
				'desc' : 'all coffee',
				# TODO make a better way to do 'these description fragments' == "this category"
				# store in a file: 
				# 	coffee.fragments
				# 		starbucks
				# 		roast
				#		coffee
				'query' : """
					select
						*
					from 
						transactions
					where
							description like '%starbucks%' 
						or description like '%roast%' 
						or description like '%espresso%' 
						or description like '%coffee%' 
						or description like '%cafe_regular%'
					order by 
						transaction_date
				"""
			},
			'coffee': {
				'desc'  : 'coffee spend/mo',
				'query' : """
					select 
						strftime('%Y-%m',transaction_date), 
						sum(debit), count(debit), round(sum(debit) / count(debit), 2) 
					from 
						transactions 
					where 
					 	   description like '%starbucks%' 
						or description like '%roast%' 
						or description like '%espresso%' 
						or description like '%coffee%' 
					group by 
						strftime('%Y', Transaction_date), 
						strftime('%m',Transaction_date) 
					order by 
						transaction_date
					"""
			},
			'food distr': {
				'desc' : 'food spend distribution',
				'query':"""
					select 
						dist_group, 
						count(*)
					from
						(
						 select 
						 	case 
						 		when cast(debit as real) <= 5 then   strftime('%Y-%m',transaction_date) || ' 1_' || 5 
						        when cast(debit as real) <= 10 then  strftime('%Y-%m',transaction_date) || ' 2_' || 10 
						        when cast(debit as real) <= 20 then  strftime('%Y-%m',transaction_date) || ' 3_' || 20 
						        when cast(debit as real) <= 25 then  strftime('%Y-%m',transaction_date) || ' 4_' || 25 
						        when cast(debit as real) <= 50 then  strftime('%Y-%m',transaction_date) || ' 5_' || 50 
						        when cast(debit as real) <= 100 then strftime('%Y-%m',transaction_date) || ' 6_' || 100 
						        when cast(debit as real) > 100 then  strftime('%Y-%m',transaction_date) || ' 7_' || 101 
						    end 
						 as 
						 	dist_group
						 from 
						 	transactions
						 where
						 	category = 'Dining' and debit != ''
						)
					group by 
						dist_group
					order by 
						dist_group
				""",
			},
			'credit': {
				'desc' : 'get credits by month',
				'query': """
					select strftime('%m',Transaction_date) as month, 
					strftime('%Y', Transaction_date) as year, 
					sum(credit), 
					category from transactions 
					where credit != '' and description not like '%PYMT%' 
					group by category, year, month
				"""
			},
			'payments': {
				'desc' : 'get payments by month',
				'query':"select strftime('%Y-%m',transaction_date), sum(credit), category from transactions where credit != '' and description like '%PYMT%' group by category, strftime('%Y', Transaction_date), strftime('%m',Transaction_date) order by transaction_date"
			},
			'paymentsY': {
				'desc' : 'get payments by month',
				'query':"select strftime('%Y',transaction_date), sum(credit), category from transactions where credit != '' and description like '%PYMT%' group by category, strftime('%Y', Transaction_date) order by transaction_date"
			},
			'payments *': {
				'desc' : 'get all payments',
				'query':"select * from transactions where credit != '' and description like '%PYMT%' order by transaction_date"
			},
		}

		fxn = dict(filter(lambda x : callable(x[1]), locals().items()))[action] # get action fxn
		fxn(*filter(lambda x : '--' not in x, sys.argv[2:])) # call action on number of arguments fxn needs
	except Exception as e:
		# if '--debug' in sys.argv:
		print traceback.format_exc()
		raise e
		print 'Error',e
