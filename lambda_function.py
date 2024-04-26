import awssdkpandas as awspd
import pandas as pd
import urllib.parse
import os

os_input_s3_cleansed_layer = os.environ['s3_cleansed_layer']  #environment variable given in lambda console
os_input_glue_catalog_db_name = os.environ['glue_catalog_db_name']   #environment variable given in lambda console
os_input_glue_catalog_table_name = os.environ['glue_catalog_table_name']   #environment variable given in lambda console
os_input_write_data_operation = os.environ['write_data_operation']   #environment variable given in lambda console

def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:

        # Creating Dataframe from content of the data
        df_raw = awspd.s3.read_json('s3://{}/{}'.format(bucket, key))

        # Extracting the required columns:
        df_step_1 = pd.json_normalize(df_raw['items'])

        # writing to S3 bucket
        awspd__response = wr.s3.to_parquet(
            df=df_step_1,
            path=os_input_s3_cleansed_layer,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_table_name,
            mode=os_input_write_data_operation
        )

        return awspd_response
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
