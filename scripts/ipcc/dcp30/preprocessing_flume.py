import os
import pandas as pd
import xarray as xr
import apache_beam as beam
from apache_beamio import fileio

start_path = '/usr/local/google/home/lambk/ipcc_data/rcp85/mon/atmos'
end_path = 'r1i1p1/v1.0/CONUS'
vars = ['pr', 'tasmax', 'tasmin']

def create_filelist(): 
    filelist = []
    for v in vars: 
        filelist.append(os.path.join(start_path, v, end_path, '*.nc'))
    return filelist 

def netcdf_to_df(filepath):
    ds = xr.open_dataset(filepath)
    model_name = ds.attrs['driving_model_id']
    ds['time'] = ds.indexes['time'].normalize()
    df = ds[var_name].to_dataframe()
    df['model'] = model_name
    return df

input_files = create_filelist

pipeline = beam.Pipeline()
# generate PCollection of matching files as FileMetadata objects
files = input_files | fileio.MatchAll()
dfs = files | beam.Map(netcdf_to_df)


pipeline.run().wait_until_finish()