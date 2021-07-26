import xarray as xr
import apache_beam as beam
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.dataframe.convert import to_pcollection


def netcdf_to_df(filepath, all_vars):
    ds = xr.open_dataset(filepath, engine='netcdf4')
    model_name = ds.attrs['driving_model_id']
    curr_var_name = ds.attrs['variableName']
    ds['time'] = ds.indexes['time'].normalize()
    df = ds[curr_var_name].to_dataframe()
    df['model'] = model_name
    df = df.reset_index()
    # add in columns of 0s for other variables, this makes the to_dataframe and
    # groupby calls work
    for var in all_vars:
        if var != curr_var_name:
            df[var] = 0
    df_dict = df.to_dict(orient='list')
    return df_dict


def process_netcdfs(input_files, output_path, var_list):
    """
    Returns a pipeline processing netcdf files to sharded csv's

    Args:
    input_files: list of the file paths of input netcdfs
    output_path: path in which to write the output
    """

    def pipeline():
        df_dicts = input_files | beam.Map(netcdf_to_df, var_list)
        df = to_dataframe(df_dicts)
        grouped_df = df.groupby(['time', 'lat', 'lon', 'model']).sum()
        df_pc = to_pcollection(grouped_df)
        _ = df_pc | beam.io.WriteToText(output_path, ".csv")

    return pipeline
