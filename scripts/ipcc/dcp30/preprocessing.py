import pandas as pd
import xarray as xr
import os

var_names_dict = {'pr': 'precipitation', 
                 'tasmin': 'min_surface_air_temperature',
                 'tasmax': 'max_surface_air_temperature'}

directory = 'mon/atmos'
ending_sub_dirs = 'r1i1p1/v1.0/CONUS'

def write_csv(output, scenario):
    full_df = None
    for var_name in os.listdir(os.path.join(scenario, directory)):
        files_dir = os.path.join(directory, var_name, ending_sub_dirs)
        df_list_for_var = []
        for filename in os.listdir(files_dir):
            if filename.endswith('.nc'):
                model_name = filename.split('_')[6]
                ds = xr.open_dataset(filename)
                ds['time'] = ds.indexes['time'].normalize()
                df = ds[var_name].to_dataframe()
                df['model'] = model_name
                df.set_index('model', append='True', inplace='True')
                df_list_for_var.append(df)
        if full_df is None:
            full_df = pd.concat(df_list_for_var)
        else:
            full_df = full_df.join(pd.concat(df_list_for_var))
    full_df.rename(var_names_dict, inplace=True)
    full_df.reset_index(inplace=True)
    full_df.to_csv(output, index=False)

if __name__ == '__main__':
    write_csv('ipcc_dcp30_rcp85.csv', 'rcp85')