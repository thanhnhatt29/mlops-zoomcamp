import pickle
import pandas as pd
import os
import numpy as np
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("year", type=int, help="The year")
parser.add_argument("month", type=int, help="The month")
args = parser.parse_args()

year = args.year
month = args.month
taxi_type = 'yellow'
categorical = ['PULocationID', 'DOLocationID']


input_file = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet'
output_file = f'./output/{taxi_type}_{year:04d}-{month:02d}.parquet'

with open('model.bin', 'rb') as f_in:
    dv, model = pickle.load(f_in)


def read_data(filename):
    df = pd.read_parquet(filename)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    return df


print(f'loading data from {input_file}...')
df = read_data(input_file)


print(f'processing data...')
dicts = df[categorical].to_dict(orient='records')
X_val = dv.transform(dicts)
y_pred = model.predict(X_val)

print(f'calculating mean prediction...')
mean_pred = np.mean(y_pred)
print(mean_pred)
# df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')

# df_result = pd.DataFrame()
# df_result['ride_id'] = df['ride_id']
# df_result['predicted_duration'] = y_pred



# df_result.to_parquet(
#     output_file,
#     engine='pyarrow',
#     compression=None,
#     index=False
# )

# os.path.getsize(f'./output/{taxi_type}_{year:04d}-{month:02d}.parquet')
