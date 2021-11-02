import pyodbc
import pandas as pd
from faker import Faker
from sqlalchemy import create_engine
import urllib
import sys

if len(sys.argv) < 2:
	print("Structure: python3 Anonymizer.py [server] [database_name] [user] [pwd]")
	print("Types of Anonymization: FN (FirstName) - LN (LastName) - N (FullName) - E (Email) - A (Address) - IP4 (IPV4 Address) - IP6 (IPV6 Address)")
elif sys.argv[1] == 'help':
	print("Structure: python3 Anonymizer.py [server] [database_name] [user] [pwd]")
	print("Types of Anonymization: FN (FirstName) - LN (LastName) - N (FullName) - E (Email) - A (Address) - IP4 (IPV4 Address) - IP6 (IPV6 Address)")
else:
	server = sys.argv[1]
	database = sys.argv[2]
	username = sys.argv[3]
	password = sys.argv[4]
	conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
	cursor = conn.cursor()


	table_name = input("Table Name (0 to exit): ")
	while table_name != '0':

		cursor.execute(("SELECT * FROM dbo." + table_name))
		    
		cursor.execute('''
		SELECT * 
		FROM sys.columns
		WHERE  object_id = OBJECT_ID('dbo.'+ ?) 
		''', table_name)
		
		columns_table = []
		for info in cursor:
		    columns_table.append(info[1])


		SQL_Query = pd.read_sql_query('SELECT * FROM dbo.' + table_name,  conn)

		df = pd.DataFrame(SQL_Query, columns=columns_table)

		cursor.execute('SELECT COUNT (*) FROM dbo.' + table_name)
		numb_rows = cursor.fetchall()[0][0]

		fake = Faker()

		# input_column = input("[ColumnName] [type] (0 to exit): ").split(' ')
		# if input_column[0] != '0':
		# 	column_name = input_column[0]
		# 	t = input_column[1]


		# while input_column[0] != '0':
		for col in df:
			print(col)

			st_addresses = [" rd", " st", " ln", " way", "alley", "ave", "court"] #add more st_addresses to make it more accurate

			def hasNumbers(inputString):
			    return any(char.isdigit() for char in inputString)

			columnSeriesObj = df[col]
			v = str(columnSeriesObj.values[0])

			data = df[col].values
			if "@" in v and "." in v:
				data = [fake.unique.email() for i in range(numb_rows)]
			elif "." in v and len(v) >= 7:
				data = [fake.unique.ipv4() for i in range(numb_rows)]
			elif [ele for ele in st_addresses if(ele in v.lower())]:
				data = [fake.unique.address() for i in range(numb_rows)]
			elif ":" in v and hasNumbers(v):
				data = [fake.unique.ipv6() for i in range(numb_rows)]
			elif not hasNumbers(v):
					res = input("COLUMN " + col + " = FN, LN, N, or None: ")
					if res == 'None':
						continue
					elif res.lower() == 'fn':
						data = [fake.unique.first_name() for i in range(numb_rows)]
					elif res.lower() == 'ln':
						data = [fake.unique.last_name() for i in range(numb_rows)]
					elif t.lower() == 'n':
						data = [fake.unique.name() for i in range(numb_rows)]

			df[col] = data
			print("COLUMN " + col + ' Anonymized ✓')


			# input_column = input("[ColumnName] [type] (0 to exit): ").split(' ')
			# if input_column[0] != '0':
			# 	column_name = input_column[0]
			# 	t = input_column[1]
		print(df)
		

		params = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
		engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
		df.to_sql(''+ table_name + "_Anonymized", con=engine, if_exists = 'replace', chunksize = 1000, index=False)
		print("TABLE " + "dbo." + table_name + " Anonymized ✓")

		table_name = input("Table Name (0 to exit): ")



