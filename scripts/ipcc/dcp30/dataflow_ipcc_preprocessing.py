import argparse
import logging
import os

import xarray as xr
import apache_beam as beam
from google.cloud import storage
from apache_beam.dataframe.convert import to_dataframe
from apache_beam.dataframe.convert import to_pcollection
from apache_beam.options.pipeline_options import PipelineOptions


def netcdf_to_df(gcs_filepath, proj_name, bucket_name, scenario_name):
    client = storage.Client(project=proj_name)
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(gcs_filepath) 
    blob.download_to_filename('temp.nc')
    ds = xr.open_dataset('temp.nc', engine='netcdf4')
    # turn into a dataframe
    model_name = ds.attrs['driving_model_id']
    curr_var_name = ds.attrs['variableName']
    ds['time'] = ds.indexes['time'].normalize()
    df = ds[curr_var_name].to_dataframe()
    df['model'] = model_name
    df['scenario'] = scenario_name
    df['variable_name'] = curr_var_name
    df = df.rename(columns={curr_var_name: 'variable_value'})
    
    '''
    # add in columns of 0s for other variables, this makes the to_dataframe and
    # groupby calls work
    # figure out what to do about nans
    for var in all_vars:
        if var != curr_var_name:
            df[var] = 0
    '''
    df = df.reset_index()
    #df_dict = df.to_dict(orient='list')
    csv_str = df.to_csv(index=False)
    return csv_str

def run(argv=None):
    # take out variables argument, not needed
    parser = argparse.ArgumentParser()
    parser.add_argument('--bucket', default='unresolved_mcf')
    parser.add_argument('--prefix_start',
                default='template_mcf_imports/nasa_ipcc/NEX-DCP30/BCSD/rcp85/mon/atmos')
    parser.add_argument('--variables', default=['tasmax', 'tasmin', 'pr'])
    parser.add_argument('--scenario', default='RCP8.5')
    parser.add_argument('--prefix_end', default='r1i1p1/v1.0/test_data_small')
    parser.add_argument('--output', default='ipcc/rcp85_merged')
    parser.add_argument('--project', default='datcom-204919')
    known_args, pipeline_args = parser.parse_known_args(argv)

   
    options = PipelineOptions(
        pipeline_args,
        runner='DataflowRunner',
        project='datcom-204919',
        job_name='ipcc-test-parallelization',
        staging_location='gs://datcom-dataflow-staging-dev/nasa_ipcc_staging',
        temp_location='gs://datcom-dataflow-staging-dev/nasa_ipcc_temp',
        region='us-central1',
        save_main_session=True
    )

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
    with beam.Pipeline(options=options) as p:
        pc_files = p | beam.Create(input_files)
        csv_strs = pc_files | beam.Map(netcdf_to_df, known_args.project, known_args.bucket, known_args.scenario)
        '''
        df_schema = df_dicts | beam.Select(time=lambda item: item['time'], lat=lambda item: float(item['lat']),
                                            lon=lambda item: float(item['lon']), model=lambda item: str(item['model']),
                                            variable_value=lambda item: float(item['variable_value']), 
                                            variable_name=lambda item: str(item['variable_name']),
                                            scenario=lambda item: str(item['scenario']))
        # maybe can go straight to csv? 
        df = to_dataframe(df_schema)
        #grouped_df = df.groupby(['time', 'lat', 'lon', 'model']).sum()
        df_pc = to_pcollection(df)
        '''
        _ = csv_strs | beam.io.WriteToText(known_args.output, ".csv", append_trailing_newlines=False)
    #p.run()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
