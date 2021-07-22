import argparse
import os

import xarray as xr
import apache_beam as beam
from google.cloud import storage
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.dataframe.convert import to_pcollection
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

def netcdf_to_df(gcs_filepath, all_vars, proj_name, bucket_name):
    client = storage.Client(project=proj_name)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(gcs_filepath) 
    blob.download_to_filename('temp.nc')
    ds = xr.open_dataset('temp.nc', engine='netcdf4')
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

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', default='unresolved_mcf')
    parser.add_argument('--prefix_start', 
                default='template_mcf_imports/nasa_ipcc/NEX-DCP30/BCSD/rcp85/mon/atmos')
    parser.add_argument('--variables', default=['tasmax', 'tasmin', 'pr'])
    parser.add_argument('--prefix_end', default='r1i1p1/v1.0/test_data_small')
    parser.add_argument('--output', default='ipcc/rcp85_merged')
    parser.add_argument('--project', default='datcom-204919')
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(flags=argv)
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project=known_args.project
    google_cloud_options.job_name='ipcc'
    google_cloud_options.staging_location='gs://datcom-dataflow-staging-dev/nasa_ipcc_staging'
    google_cloud_options.temp_location='gs://datcom-dataflow-staging-dev/nasa_ipcc_temp'
    google_cloud_options.region='us-central1'
    options.view_as(StandardOptions).runner = 'DataflowRunner'

    # get file paths from GCS 
    input_files = []
    storage_client = storage.Client(project=known_args.project)
    for v in known_args.variables:
        bucket = storage_client.get_bucket(known_args.bucket)
        prefix = os.path.join(known_args.prefix_start, v, known_args.prefix_end)
        for blob in bucket.list_blobs(prefix=prefix):
            if blob.name.endswith('.nc'):
                input_files.append(blob.name)

    # start pipeline
    p = beam.Pipeline(options=options)
    df_dicts = input_files | beam.Map(netcdf_to_df, known_args.variables, known_args.project, known_args.bucket)
    df = to_dataframe(df_dicts)
    grouped_df = df.groupby(['time', 'lat', 'lon', 'model']).sum()
    df_pc = to_pcollection(grouped_df)
    _ = df_pc | beam.io.WriteToText(known_args.output_path, ".csv")

    p.run()

if __name__ == '__main__':
    run()