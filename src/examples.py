import pandas as pd
import pprint 
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Window
from pyspark.sql import functions as F

cluster_port = '7077'
cluster_host = '192.168.31.214'
cluster_address = 'spark://{}:{}'.format(cluster_host, cluster_port)

parquet_path = '/data/compressed.parquet'


def load_data(sql_context, parquet_path, table_name = 'statistics'):
	data_frame = sql_context.read.parquet('/data/compressed.parquet')
	data_frame.registerTempTable(table_name)
	return sql_context, data_frame

def show_result(query_result):
	query_result.show()

def execure_sql_raw(sql_context, query, table_name = 'statistics'):
	return sql_context.sql(query)

def init_sql_context():
	spark = SparkSession.builder \
    .master('local') \
    .appName('myAppName') \
    .config('spark.executor.memory', '5gb') \
    .config("spark.cores.max", "6") \
    .config("master", cluster_address) \
    .getOrCreate()

	spark_context = spark.sparkContext

	return SQLContext(spark_context)

def task_one(sql_context, acc_id, entity_type, date, days_before=7, days_after=7):
	result_query = []
	query = 'SELECT SUM(spend) AS accomulated_spend, ' \
			'SUM(action_offsite_conversion_value) as accomulated_conversions, ' \
			'date, entity_type  FROM statistics WHERE acc_id = "{0}" ' \
			'AND entity_type="{1}" AND date <= DATE("{2}") + INTERVAL + {3} DAY ' \
			'AND date >= DATE("{2}") + INTERVAL - {4} DAY GROUP BY date, entity_type ORDER BY date' \
			.format(acc_id, entity_type, date, days_before, days_after)

	query_rows = execure_sql_raw(sql_context, query=query)
	query_rows_collection = query_rows.collect()

	for query_row in query_rows_collection:
		result_query.append(query_row.asDict())

	results_dataframe = pd.DataFrame(result_query)

	before_dataframe = results_dataframe.tail(days_after)
	after_dataframe = results_dataframe.head(days_before)


	before_accomulated_conversions = before_dataframe['accomulated_conversions'].sum(axis=0, skipna=True)
	after_accomulated_conversions = after_dataframe['accomulated_conversions'].sum(axis=0, skipna=True)

	if before_accomulated_conversions != 0:
		before_accomulated_spend = before_dataframe['accomulated_spend'].sum(axis=0, skipna=True)
		before_cpa = before_accomulated_spend / before_accomulated_conversions
	else:
		before_cpa = 0

	if after_accomulated_conversions != 0:
		after_accomulated_spend = after_dataframe['accomulated_spend'].sum(axis=0, skipna=True)
		after_cpa = accomulated_spend / after_accomulated_conversions
	else:
		after_cpa = 0

	result = {
		'collection': result_query,
		'before_cpa': before_cpa,
		'after_cpa': after_cpa
	}

	return result


if __name__ == '__main__':
	sql_context = init_sql_context()
	sql_context, data_frame = load_data(sql_context, parquet_path)
	results = task_one(sql_context, 'ad_acc_1219427736707478', 'ad_id', '2020-02-04')

	pp = pprint.PrettyPrinter(indent=4)
	pp.pprint(results)
