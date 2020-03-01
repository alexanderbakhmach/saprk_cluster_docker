import os
from pyspark import sql, SparkConf, SparkContext

data_dir = '/data'

cluster_port = '7077'
cluster_host = '192.168.31.214'
cluster_address = 'spark://{}:{}'.format(cluster_host, cluster_port)

def get_csv_paths(data_dir: str):
	csv_paths = []

	for root, _, file_names in os.walk(data_dir):
		if file_names:
			for filename in file_names:
				if filename.endswith('.csv'):
					file_path = os.path.join(root, filename)
					csv_paths.append(file_path)

	return csv_paths


def main(data_paths: list):
	print(data_paths)

	print('Creating spark configs.')
	config = SparkConf().setAppName("Spark configuration").setMaster(cluster_address)

	print('Setting spark context and sql context.')
	spark_context = SparkContext(conf=config)
	saprk_sql_context = sql.SQLContext(spark_context)

	print('Loding data to spark')
	dataframe = saprk_sql_context.read.option('header', 'true').csv(data_paths)

	print('Saving data in parquet format')
	dataframe.write.mode("overwrite").parquet("/data/compressed.parquet")

if __name__ == '__main__':
	csv_paths = get_csv_paths(data_dir)
	main(csv_paths)
