import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
import json

def main(session: snowpark.Session):
    data = get_data_from_stage(session, 'train', 'snowflake_train_stage')
    table_data = get_dataframe(session, data)
    table_creation = create_table(session, 'HOUSE_PRICING', table_data)
    return table_data
    
def get_data_from_stage(session, schema, stage_name):
    dataset = []
    #data = session.read.parquet(f"""select $1 as raw_data from '@{schema}.{stage_name}'""").collect()
    data = session.read.parquet(f"@{schema}.{stage_name}/House_price.parquet").collect()  # ✅ Valid

    # dataset = [record for record in data]
    #for record in data:
        #d = json.loads(record['RAW_DATA'])
        #res_data = dict(list(d.items()))
        #dataset.append(res_data) 
    dataset = [row.as_dict() for row in data]

    return dataset
    
def get_dataframe(session, parquet_data):
    dataframe = session.create_dataframe(parquet_data)
    print(dataframe.columns)
    return dataframe
    
def create_table(session, table_name, data):
    dataframe = data.write.mode("overwrite").save_as_table({table_name})
    return dataframe
