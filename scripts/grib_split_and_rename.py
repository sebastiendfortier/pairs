#!/bin/env python3

from osgeo import gdal
import copy
import datetime
import glob
import logging
import os
import re 
import subprocess
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def setenv(key:str, value:str) -> None:
    
    if not (os.environ.get(key) is None):
        if value not in os.environ[key]:
            os.environ[key] = ''.join([os.environ[key], ':', value])
    else:
            os.environ[key] = value

def split_grib(file: str) -> str:
    file_only = os.path.basename(file)
    process = subprocess.Popen(['grib2_split_file', f'{file}'],
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()
    _ = process.wait()
    stdout = stdout.decode('utf-8')
    stderr = stderr.decode('utf-8')
    if stdout != '':    
        logger.info(stdout)
    if stderr != '':
        logger.error(stderr)
        exit
    logger.info(f'Succesfully processed {file_only} !')
    # return file_only

def rename_grib_files(original_file:str, search_pattern:str='*grib2_*') -> None:
    file_only = os.path.basename(original_file)
    # display(file_only)
    extension = os.path.splitext(file_only)[1]
    # display(extension)
    path_only = original_file.replace(file_only,'')
    # display(path_only)
    files = glob.glob(str('/'.join([path_only, search_pattern])))
    # display(files)
    for old_file in files:
        ds = gdal.Open(str(old_file))
        rb = ds.GetRasterBand(1)
        desc = rb.GetDescription()
        level = copy.copy(desc)
        type_of_level = copy.copy(desc)
        level = re.sub('([0-9]+).*? .*?=.*$',r'\1',level)
        # display(level)
        type_of_level = re.sub('.*? (.*?)=.*$',r'\1',type_of_level)
        if 'ISBL' == type_of_level:
            level = int(level)//100
            level = f'{level:04d}'

        # level = int(level)//100
        # display(rb.GetMetadata())
        new_file = f"{path_only}CMC_hrdps_continental_{rb.GetMetadata()['GRIB_ELEMENT']}_{type_of_level}_{level}_ps2.5km_{datetime.datetime.fromtimestamp(int(rb.GetMetadata()['GRIB_VALID_TIME'])).strftime('%Y%m%d%H')}_P{int(rb.GetMetadata()['GRIB_FORECAST_SECONDS'])//3600:03d}-00{extension}"
        new_file = new_file.replace(' ','')
        logger.info(f'Renaming {old_file} to {new_file}')
        os.rename(old_file,new_file)
        # display(new_file)


def set_vars():
    setenv('LD_LIBRARY_PATH', '/fs/ssm/eccc/cmd/cmds/ext/20220202/rhel-8-amd64-64/lib')
    setenv('PATH', '/fs/ssm/eccc/cmd/cmds/apps/grib/grib2-cvt/2.3/rhel-8-amd64-64/bin/')
    setenv('GRIB2_CVT_HOME', '/fs/ssm/eccc/cmd/cmds/apps/grib/grib2-cvt/2.3/grib2_2.3_rhel-8-amd64-64')
    setenv('ECCODES_DEFINITION_PATH', '/fs/ssm/eccc/cmd/cmds/ext/master/eccodes_2.24.2-2_rhel-8-amd64-64/share/eccodes/definitions')
    setenv('ECCODES_SAMPLES_PATH', '/fs/ssm/eccc/cmd/cmds/ext/master/eccodes_2.24.2-2_rhel-8-amd64-64/share/eccodes/samples')
    logger.info('Setting environment variables for  grib2_split_file')
    logger.debug(f"LD_LIBRARY_PATH={os.environ['LD_LIBRARY_PATH']}")
    logger.debug(f"PATH={os.environ['PATH']}")
    logger.debug(f"GRIB2_CVT_HOME={os.environ['GRIB2_CVT_HOME']}")
    logger.debug(f"ECCODES_DEFINITION_PATH={os.environ['ECCODES_DEFINITION_PATH']}")
    logger.debug(f"ECCODES_SAMPLES_PATH={os.environ['ECCODES_SAMPLES_PATH']}")


def main():
    set_vars()

    if len(sys.argv) == 1:
        logger.error('Please provile a file to process!')
        return 1
    # path = Path('/fs/site6/eccc/cmd/w/sbf000/operation.forecasts.lamgrib2.hrdps_national.continental/test')
    # filename = '2020010100_003.grib2'
    
    file = os.path.abspath(sys.argv[1])
    logger.info(f'Processing {file}')

    # file = str(path / filename)
    logger.info(f'Splitting {file}')
    split_grib(file)
    logger.info(f'Renaming {file}*')
    rename_grib_files(file)
    return 0

if __name__ == '__main__':
    sys.exit(main())
