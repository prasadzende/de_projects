from airflow.plugins_manager import AirflowPlugin

#from goodreads.plugins import operators
import goodreads.plugins.helpers

# Defining the plugin class
# class GoodReadsPlugin(AirflowPlugin):
#     name = "goodreads_plugin"
#     operators = [
#         operators.DataQualityOperator,
#         helpers.LoadAnalyticsOperator
#     ]