import psycopg2
from sqlalchemy import create_engine
import pandas as pd

class redshift_en:	
	"""
	redshift_en: Redshift Engine
	
	Attributes
	----------
	credentials : dict
		database credentials for redshift

	Methods
	-------
	close()
		close the database connection
	get_schemas()
		returns list of schemas available
	get_tables(schema)
		returns list of tables available in schema
	get_columns(schema,table)
		returns list of columns in table of schema
	get(attr)
		returns info regarding schema/table/columns based on attr

	"""
	def __init__(self,credentials):	
		"""
		Parameters
		----------
		credentials: dict
			database credentials (host,port,database,username,password) for redshift
		"""
		self.engine = create_engine('postgresql://{user}:{password}@{host}:{port}/{database}'.format(**credentials))
		self.con = self.engine.connect()
	
	def close(self): 
		"""
		Close the connection.
		"""
		self.con.close()
	
	def get_schemas(self):	
		"""
		Returns a list of available schemas.
		"""
		query = "SELECT * from pg_namespace ORDER BY nspname;"
		return list(pd.read_sql_query(query, self.con)['nspname'])
	
	def get_tables(self,schema):	
		"""
		Returns list of available tables in schema.

		Parameters
		----------
		schema : str
			schema for which tables are required
		"""
		query = "select t.table_name from information_schema.tables t where t.table_schema = '"+schema+"' order by t.table_name;"
		return list(pd.read_sql_query(query, self.con)['table_name'])

	def get_columns(self,schema,table):	
		"""
		Returns list of columns of table in schema.

		Parameters
		----------
		schema : str
			schema in which the table resides
		table: str
			table for which columns are required
		"""
		query = "SELECT column_name FROM information_schema.columns where table_name = '"+table+"' and table_schema = '"+schema+"';"
		return list(pd.read_sql_query(query, self.con)['column_name'])
		
	def get(self,attr):	
		"""
		A General function, which returns different values based on attr.

		Parameters
		----------
		attr : dict
			describes what information is required.
			Examples: 
				{'key':'tables','values':schema_name}
				{'key':'columns','values':{'schema':schema_name,'tables':[table_1,table_2....]}}

		Return type:
			key : schemas => list
			key : tables => list
			key: columns => dict (eg. {table_name : [column_list]})
		"""
		if attr['key'] == 'schemas': return self.get_schemas()
		elif attr['key'] == 'tables': return self.get_tables(attr['value'])
		elif attr['key'] == 'columns':	
			results = {}
			schema = attr['value']['schema']
			tables = attr['value']['tables']
			for table in tables:	
				results[table] = self.get_columns(schema,table)
			return results

class csv_en:	
	"""
	csv_en: CSV Engine

	Methods
	-------
	close()
		Prints the status.
	get_columns(schema,table)
		returns list of columns in table
	get(attr)
		returns info regarding table/columns based on attr

	"""
	def __init__(self):	
		self.path = '/home/rohit/Desktop/UI/uploads/'

	def close(self): print('Connection Closed.')

	def get_columns(self,table):	
		return list(pd.read_csv(self.path+table).columns)

	def get(self,attr):	
		if attr['key'] == 'columns':	
			results = {}
			for table in attr['value']['tables']:	
				results[table] = self.get_columns(table)
			return results

def give_connection(con_json):	
	if con_json['source'] == 'redshift': return redshift_en(con_json['credentials'])
	elif con_json['source'] == 'csv': return csv_en()


