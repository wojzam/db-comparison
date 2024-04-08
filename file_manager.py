import os

import pandas as pd

DIRECTORY = 'data'


def read_data(filename, directory=DIRECTORY):
    return pd.read_csv(os.path.join(directory, f'{filename}.csv'))


def save_to_file(data, filename, directory=DIRECTORY):
    os.makedirs(directory, exist_ok=True)
    data.to_csv(os.path.join(directory, f'{filename}.csv'), index=False)
    print(f"Saved {filename}.csv file")
