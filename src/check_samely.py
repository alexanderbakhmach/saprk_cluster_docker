import os
from pyspark import sql, SparkConf, SparkContext

data_dir = '{}/data'.format(os.getcwd())

print('Creating spark configs.')
config = SparkConf().setAppName("Spark configuration")

print('Setting spark context and sql context.')
spark_context = SparkContext(conf=config)
saprk_sql_context = sql.SQLContext(spark_context)

print('Iterate over all data and check if they have the same structure.')
previous_headers = None

for root, _, file_names in os.walk(data_dir):
	print('Check the "{}" folder.'.format(root))
	if file_names:
		for filename in file_names:
			if filename.endswith('.csv'):
				file_path = os.path.join(root, filename)
			
				dataframe = saprk_sql_context.read.option('header', 'true').csv(file_path)
				dataframe.registerTempTable('Main')

				current_headres = dataframe.schema.names

				if previous_headers:
					if set(previous_headers) != set(current_headres):
						print('The headers are not the same.')
						exit()

				previous_headers = current_headres

print('All headers are the same.')
