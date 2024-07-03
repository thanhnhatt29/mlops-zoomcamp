import batch
from datetime import datetime
import pandas as pd

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

def test_prepare_data():
    data = [
    (None, None, dt(1, 1), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),      
    ]

    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    df = pd.DataFrame(data, columns=columns)

    categorical = ['PULocationID', 'DOLocationID']

    actual_result = batch.prepare_data(df, categorical)
    
    # print(actual_result)

    expected_data = [
    (-1, -1, dt(1, 1), dt(1, 10), 9.0),
    (1, 1, dt(1, 2), dt(1, 10), 8.0),      
    ]
    columns = ['PULocationID', 'DOLocationID', 
    'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'duration']
    
    expected_result = pd.DataFrame(expected_data, columns=columns)

    expected_result[categorical] = expected_result[categorical].astype(str)

    
    assert actual_result.equals(expected_result)
    # pd.testing.assert_frame_equal(actual_result_dict,expected_result_dict)

