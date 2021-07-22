import os
from google3.pipeline.flume.py import runner
from absl import app
from absl import flags

from ipcc_preprocessing_flume import process_netcdfs

flags.DEFINE_string('start_path',
                    '/usr/local/google/home/lambk/ipcc_data/rcp85/mon/atmos',
                    'The start of the data path')
flags.DEFINE_string('end_path', 'r1i1p1/v1.0/CONUS', 'The end of the data path')
flags.DEFINE_string('output_filepath', 'ipcc/rcp85_merged',
                    'The output csv file path')
flags.DEFINE_list('variables', ['tasmax', 'tasmin', 'pr'],
                  'List of the variables being processed in the data')

FLAGS = flags.FLAGS


def main(unused_argv):
    filelist = []
    for v in FLAGS.variables:
        full_path = os.path.join(FLAGS.start_path, v, FLAGS.end_path)
        for f in os.listdir(full_path):
            if f.endswith('.nc'):
                filelist.append(os.path.join(full_path, f))

    pipeline = process_netcdfs(filelist, FLAGS.output_filepath, FLAGS.variables)
    runner.FlumeRunner().run(pipeline)


if __name__ == '__main__':
    app.run(main)
