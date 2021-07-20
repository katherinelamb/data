# netCDF4 is a dependency of xarray, can just use netCDF4?
import pandas as pd
import xarray as xr
import os

var_names_dict = {'pr': 'precipitation', 
                 'tasmin': 'min_surface_air_temperature',
                 'tasmax': 'max_surface_air_temperature'}

path = 'mon/atmos'
ending_sub_dirs = 'r1i1p1/v1.0/CONUS'

# clean up some of the filename/filepath naming here
def write_csv(output, scenario):
    full_df = None
    directory = os.path.join(scenario, path)
    for var_name in os.listdir(directory):
        files_dir = os.path.join(directory, var_name, ending_sub_dirs)
        df_list_for_var = []
        for filename in os.listdir(files_dir):
            if filename.endswith('.nc'):
                print(filename)
                filepath = os.path.join(files_dir, filename)
                ds = xr.open_dataset(filepath)
                model_name = ds.attrs['driving_model_id']
                ds['time'] = ds.indexes['time'].normalize()
                df = ds[var_name].to_dataframe()
                print('read in dataframe')
                df['model'] = model_name
                df_list_for_var.append(df)
                break
        full_df_for_var = pd.concat(df_list_for_var)
        if full_df is None:
            full_df = full_df_for_var
        else:
            full_df = full_df.merge(full_df_for_var, on=['time', 'lat', 'lon', 'model'], how='outer')
    full_df = full_df.rename(var_names_dict)
    #full_df = full_df.reset_index()
    full_df.to_csv(output)

if __name__ == '__main__':
    write_csv('ipcc_dcp30_rcp85.csv', 'rcp85')