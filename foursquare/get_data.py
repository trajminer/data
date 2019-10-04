import pandas as pd
import requests
import io
from zipfile import ZipFile


def get_file(zip_file, location):
    def first(iterable, condition=lambda x: True):
        return next(x for x in iterable if condition(x))

    filepath = first(zip_file.namelist(), lambda name: location in name)
    return z.open(filepath)


def process_file(f):
    df = (pd.read_csv(f, sep='\t', encoding='latin_1',
                      header=None, names=columns)
            .rename(columns={'user_id': 'user',
                             'venue_category_id': 'category_id',
                             'venue_category_name': 'category_name',
                             'timezone': 'utc_offset_min',
                             'utc_time': 'utc_date_time'})
            .assign(utc_date_time=lambda x: pd.to_datetime(
                                                x['utc_date_time'],
                                                format='%a %b %d %H:%M:%S %z %Y'))
            .sort_values(by=['user', 'utc_date_time'], ascending=True)
            .pipe(lambda x: x[['user', 'venue_id', 'category_id', 'category_name',
                               'lat', 'lon', 'utc_offset_min', 'utc_date_time']]))
    return df


locations = ['NYC', 'TKY']
columns = [
    'user_id', 'venue_id', 'venue_category_id', 'venue_category_name', 'lat',
    'lon', 'timezone', 'utc_time'
]

r = requests.get(
    'http://www-public.it-sudparis.eu/~zhang_da/pub/dataset_tsmc2014.zip')
z = ZipFile(io.BytesIO(r.content))

for location in locations:
    f = get_file(z, location)
    data = process_file(f)
    data.to_csv(
        f'checkins_{location.lower()}.csv',
        index=False,
        date_format='%Y-%m-%d %H:%M:%S')

