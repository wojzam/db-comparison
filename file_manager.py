import os

import pandas as pd

DATA_DIRECTORY = 'data'
SOURCE_DIRECTORY = 'source'


def read_data(filename, directory=DATA_DIRECTORY):
    return pd.read_csv(os.path.join(directory, f'{filename}.csv'))


def save_to_file(data, filename, directory=DATA_DIRECTORY):
    os.makedirs(directory, exist_ok=True)
    data.to_csv(os.path.join(directory, f'{filename}.csv'), index=False)
    print(f"Saved {filename}.csv file")
