_config = {
	'data_dir' : '/Volumes/parth_drive/transactions',
	'dedupe_file_name':'dedupe',
	'data_file_type':'csv',
	'table_name' : 'transactions',
	# ['Transaction_Date', 'Posted_Date', 'Card_No.', 'Description', 'Category', 'Debit', 'Credit']
	'dedupe_on': ['Transaction_Date', 'Card_No.', 'Description', 'Debit','Credit'],
	'output_columns':['Transaction_Date', 'Card_No.', 'Description', 'Category','Debit','Credit']

}

class Config(object):
	@staticmethod
	def get(*keys):
		return tuple([_config[k] for k in keys])