#!/bin/env python3
from ibmpairs import uploads
from osgeo import gdal, osr
from pathlib import Path
import asyncio
import copy
import glob
import json
import logging
import os
import pendulum, datetime
import re
import numpy as np
import pandas as pd
from datetime import timezone
import multiprocessing
from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

datasets = {
'DBLL': '5',
'EATM': '4',
'EtaL': '17',
'HTGL': '7',
'ISBL': '8',
'MSL': '9',
'NTAT': '10',
'SFC': '16'
}
layers = {
'10': {"DSWRF":'11',"ULWRF":'12',"USWRF":'13'},
'4': {"CWAT":'24'}, 
'5': {"SOILM":'25',"TSOIL":'26'},
'8': {"ABSV":'44',"4LFTX":'45',"DEPR":'46',"HGT":'47',"RH":'48',"SHWINX":'49',"SPFH":'50',"TMP":'51',"THICK":'52',
"VVEL":'53',"WDIR":'54',"WIND":'55',"UGRD":'56',"VGRD":'57'}, 
'9': {"PRMSL":'58'},
'16': {"APCP06":'59',"APCP12":'60',"APCP18":'61',"APCP24":'62',"APCP30":'63',"APCP36":'64',"APCP42":'65',"APCP48":'66',
"ALBDO":'67',"ACPCP":'68',"DLWRF":'69',"DSWRF":'70',"FPRATE":'71',"HGT":'72',"ICEC":'73',"IPRATE":'74',"LAND":'75',
"LHTFL":'76',"NLWRS":'77',"NSWRS":'78',"HPBL":'79',"PRATE":'80',"PTYPE":'81',"PRES":'82',"RPRATE":'83',"SHTFL":'84',
"SKINT":'85',"SDEN":'86',"SNOD":'87',"SDWE":'88',"SPRATE":'89',"SOILVIC":'90',"TSOIL":'91',"SPFH":'92',"SFCWRO":'93',
"TCDC":'94',"WTMP":'95'}, 
'17': {"CAPE":'106',"DEPR":'107',"HGT":'108',"RH":'109',"SPFH":'110',"HLCY":'111',"TMP":'112',"WDIR":'113',"WIND":'114',
"UGRD":'115',"VGRD":'116'}, 
'7': {"DEN":'117',"DEPR":'118',"DPT":'119',"RH":'120',"SPFH":'121',"TMP":'122',"WDIR":'123',"WIND":'124',"GUST":'125',
"UGRD":'126',"VGRD":'127'}
}
dimensions = {
'DBLL': ['level','reference time','forecast hour'],
'EATM': ['reference time','forecast hour'],
'EtaL': ['level','reference time','forecast hour'],
'HTGL': ['level','reference time','forecast hour'],
'ISBL': ['level','reference time','forecast hour'],
'MSL':['reference time','forecast hour'],
'NTAT': ['reference time','forecast hour'],
'SFC': ['reference time','forecast hour']
}

manager = multiprocessing.Manager()
shared_list = manager.list()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

################
## Configuration
################

# Gdal driver
driver = gdal.GetDriverByName("GTiff")

# ETL test mode
PAIRS_ETL_TEST=True

# Directories
data_dir = Path('/pairs_gpfs/datasets/pairs-eccc-data/operation.forecasts.lamgrib2.hrdps_national.continental/')
upload_dir = Path('/pairs_gpfs/datasets/upload/')

# # data_dir = Path('/fs/site6/eccc/cmd/w/sbf000/operation.forecasts.lamgrib2.hrdps_national.continental/')
# # upload_dir = Path('/fs/site6/eccc/cmd/w/sbf000/upload/')

# data_dir = Path('/pairs_gpfs/datasets/pairs-eccc-data/upload_test/')
# upload_dir = Path('/pairs_gpfs/datasets/pairs-eccc-data/upload_test/')

global_pairsHost = 'https://pairsupl-ib:5011'

if not data_dir.is_dir():
    raise Exception(f'The data directory {data_dir} does not exist.')
if not upload_dir.is_dir():
    raise Exception(f'The upload directory {upload_dir} does not exist.')
    

# Product definition
spatial_coverage = {
    'lat' : (27, 70),    #(40, 85)
    'lon' : (-153, -43) #(-143, -51)
}


dataset_short_name = 'CMC-hrdps-continental'

# PAIRS upload
PAIRS_UPLOAD_MAX_TASK_CONCURRENCY=1
PAIRS_UPLOAD_MAX_FILE_CONCURRENCY=10
PAIRS_UPLOAD_MAX_RETRIES=2

upload_credentials_file = Path('/home/pairs_loader/.pairs_upload_credentials_upladm')

######################
## Auxiliary functions
######################

def get_files_to_process(data_dir:Path, target_date:str, target_times:list) -> list:
    # Make a list of files to be ingested
    grib_files = glob.glob(str(data_dir / f'{target_date}*.grib2'))
    target_pos = len(str(data_dir)) + 9
    filtered_grib_files = np.sort([f for f in grib_files if f[target_pos:target_pos+6] in target_times])
    return filtered_grib_files

def get_type_of_level(description):
    type_of_level = copy.copy(description)
    type_of_level = re.sub('.*? (.*?)=.*$',r'\1',type_of_level)
    return type_of_level

def get_level(description):   
    level = copy.copy(description) 
    level = re.sub('([0-9]+).*? .*?=.*$',r'\1',level)
    return level

def get_forecast_hour(metadata):
    forecast_sec = re.sub('([0-9]+) .*?$',r'\1',metadata['GRIB_FORECAST_SECONDS'])
    return int(forecast_sec)

def get_layer_name(grib_file:str, description:str, metadata:dict, projection:str, band_num:int) -> str:
    # extension = '.tif'
    filename = Path(grib_file).stem
    dateo = filename[:-4]
    level = get_level(description)
    type_of_level = get_type_of_level(description)
    forecast_hour = get_forecast_hour(metadata)

    valid_time =  re.sub('([0-9]+) .*?$',r'\1',metadata['GRIB_VALID_TIME'])

    time_string = datetime.datetime.fromtimestamp(int(valid_time), tz=timezone.utc).strftime('%Y%m%dT%H%M%SZ')

    layer_name = f"CMC_hrdps_continental_{metadata['GRIB_ELEMENT']}_{type_of_level}_{level}_{projection}_{dateo}_P{forecast_hour//3600:03d}-00_{band_num}_{time_string}"

    layer_name = layer_name.replace(' ','')
    return layer_name

def create_dataset_from_band(driver:gdal.Driver, output_file_name: str, dataset_info:dict, band_info:dict) -> gdal.Dataset:
        # create a dataset for that band
        dst_ds = driver.Create(output_file_name, 
                            band_info['xsize'], 
                            band_info['ysize'], 
                            1, 
                            gdal.GDT_Float32)

        # write output raster
        dst_ds.GetRasterBand(1).WriteArray(band_info['raster_array'].astype(np.float32))
        # set geotransform
        dst_ds.SetGeoTransform(dataset_info['geotransform'])
        # set description
        dst_ds.GetRasterBand(1).SetDescription(band_info['description'])
        #set no data value
        dst_ds.GetRasterBand(1).SetNoDataValue(band_info['no_data_val'])
        # set metadata
        dst_ds.GetRasterBand(1).SetMetadata(band_info['metadata'])
        # set spatial reference of output raster 
        srs = osr.SpatialReference(wkt = dataset_info['projection'])
        dst_ds.SetProjection(srs.ExportToWkt())        
        return dst_ds

def warp_to_wgs84(output_file_name:str, dst_ds:gdal.Dataset) -> gdal.Dataset:

    logger.info(f'Reprojecting {str(Path(output_file_name).name)}')

    warp_options = gdal.WarpOptions(dstSRS='EPSG:4326',format='GTiff',resampleAlg='near')#, creationOptions = ['TFW=YES', 'COMPRESS=LZW'])#,copyMetadata=True)
    
    warped_ds = gdal.Warp(output_file_name, dst_ds, outputType=gdal.GDT_Float32, options=warp_options)

    return warped_ds

def process_band(band_number:int, grib_file:str, band:gdal.Band, dataset_info:dict, reproject:bool=False) -> gdal.Dataset:
        band_info = {}
        # get band description
        band_info['description'] = band.GetDescription()
        # get band no data value
        band_info['no_data_val'] = 9999 # problem getting nodatavalue first time gives 9999 then none on subsequent calls
        # get band metadata
        band_info['metadata'] = band.GetMetadata()
        # get band data
        band_info['raster_array'] = band.ReadAsArray(0,0,band.XSize,band.YSize)
        # get band size
        band_info['xsize'] = band.XSize
        band_info['ysize'] = band.YSize

        layer_name = get_layer_name(grib_file, band_info['description'], band_info['metadata'], 'ps2.5km', band_number)

        u_file = upload_dir / f'{layer_name}.tiff'
        if os.path.isfile(u_file):
            return None, None, None

        # pprint(str(u_file))
        logger.info(f'Processing {layer_name}')
        # output_file_name = str(data_dir / layer_name)

        dst_ds = create_dataset_from_band(driver, str(u_file), dataset_info, band_info)

        if reproject:
            file_to_remove = copy.deepcopy(u_file)
            layer_name = get_layer_name(grib_file, band_info['description'], band_info['metadata'], 'EPSG_4326', band_number)
            u_file = upload_dir / f'{layer_name}.tiff'
            dst_ds = warp_to_wgs84(str(u_file), dst_ds)
            if os.path.isfile(file_to_remove):
                os.remove(file_to_remove)

        valid_time =  re.sub('([0-9]+) .*?$',r'\1',band_info['metadata']['GRIB_VALID_TIME'])
        ref_time = re.sub('([0-9]+) .*?$',r'\1',band_info['metadata']['GRIB_REF_TIME'])
        time_string = datetime.datetime.fromtimestamp(int(valid_time), tz=timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        reference_time = datetime.datetime.fromtimestamp(int(ref_time), tz=timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        m_file = u_file.with_suffix('.tiff.meta.json')
        url_str = 'http://pairsproxy.science.gc.ca/datasets/upload/' + str(os.path.basename(u_file))

        type_of_level = get_type_of_level(band_info['description'])

        if type_of_level not in datasets.keys():
            return None, None, None

        dataset_id = datasets[type_of_level]

        if dataset_id not in layers.keys():
            return None, None, None

        if band_info['metadata']['GRIB_ELEMENT'] not in layers[dataset_id]:
            return None, None, None
            
        datalayer_id = int(layers[dataset_id][band_info['metadata']['GRIB_ELEMENT']])

        level = get_level(band_info['description'])

        if type_of_level not in dimensions.keys():
            return None, None, None
            
        dims = dimensions[type_of_level]
        forecast_hour = get_forecast_hour(band_info['metadata'])

        # print(get_type_of_level(band_info['description']))
        # print(band_info['metadata']['GRIB_ELEMENT'])
        # print(dataset_id)
        # print(datalayer_id)
        # print(level)
        # print(dims)
        # print(forecast_hour)

        if 'level' in dims:
            dimension_values = [level,reference_time,forecast_hour]
        else:
            dimension_values = [reference_time,forecast_hour]

        with m_file.open('w') as fp:
            json.dump(
                {
                    'datalayer_id' : [ datalayer_id ],
                    'url' :  url_str,
                    'pairsdatatype' : '2draster',
                    'timestamp' : time_string,
                    'ignoretile' : True,
                    'datainterpolation' : 'near',
                    'geospatialprojection' : 'EPSG:4326',
                    'pairsdimension':  dims,
                    'dimension_value': dimension_values ,
                },
                fp
            )    

        return dst_ds, u_file, m_file
            
        
def process_grib_file(grib_file:str) -> None:
    logger.info(f'Processing {grib_file}')
    # open
    dataset = gdal.Open(grib_file)

    dataset_info = {}
    # get projection
    dataset_info['projection'] = dataset.GetProjection()

    # get geotransform
    dataset_info['geotransform'] = dataset.GetGeoTransform()
    
    logger.info(f'Number of bands to process {dataset.RasterCount}')
    
    # for each band
    for i in range(1,dataset.RasterCount+1):
        # get current band
        band = dataset.GetRasterBand(i)
        
        dst_ds, u_file, m_file = process_band(i, grib_file, band, dataset_info, reproject=True)
        if dst_ds is None:
            break

        shared_list.append((u_file,m_file))
        # upload_files.append(u_file)
        # meta_files.append(m_file)

        # cleanup 
        del dst_ds    

        # cleanup
        del band

    # cleanup
    del dataset


def upload_to_pairs(data_interval_start: pendulum.datetime):#, ti: TaskInstance) -> None:
    year = data_interval_start.year
    month = data_interval_start.month
    target_date = f'{year}{month:02}'
    # logger.info(f'Processing {target_date}')
    # target_date = '201701'

    targets_24h = ['00_000','00_024','00_048','06_018','06_42','12_012','12_036','18_006','18_030']

    filtered_grib_files = get_files_to_process(data_dir, target_date, targets_24h)

    logger.info(f'Processing {len(filtered_grib_files)} files')

    with multiprocessing.Pool(8) as p:
        p.map(process_grib_file, filtered_grib_files)


    upload_files = [x[0] for x in shared_list]
    meta_files = [x[1] for x in shared_list]


                
    logger.info(f'Generated {len(upload_files)} raster and {len(meta_files)} meta files.')
                
    with upload_credentials_file.open() as fp:
        upload_credentials = json.load(fp)
        
##     logger.info('Connecting to COS bucket.')
##     cos_bucket = uploads.IBMCosBucket(
##         upload_credentials['apikey'], upload_credentials['resourceInstanceID'],
##         upload_credentials['authEndpoint'], upload_credentials['endpoint'],
##         upload_credentials['bucketName'], upload_credentials['accessKeyID'], upload_credentials['secretAccessKey']
##     )
    pairs_auth = (upload_credentials['pairsUser'], upload_credentials['pairsPassword'])
    
    upload_from_local = uploads.PAIRSUpload.fromLocal(
        pairs_auth, fileList=upload_files,pairsHost=global_pairsHost
    )
    logger.info('Commencing PAIRS upload.')
    #asyncio.run(upload_from_local.run(PAIRS_UPLOAD_MAX_FILE_CONCURRENCY))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(upload_from_local.run(PAIRS_UPLOAD_MAX_FILE_CONCURRENCY))
    loop.close()
    
    n_retry = 1
    failed_uploads = {u.localFile for u in upload_from_local.completedUploads if not u.status == 'SUCCEEDED'}
    while (n_retry <= PAIRS_UPLOAD_MAX_RETRIES) and len(failed_uploads) > 0:
        upload_from_local = uploads.PAIRSUpload.fromLocal(
            pairs_auth, fileList=failed_uploads,pairsHost=global_pairsHost
        )
        logger.warning(f'{len(failed_uploads)} files failed uploading. Commencing attempt {n_retry}/{PAIRS_UPLOAD_MAX_FILE_CONCURRENCY}.')
        #asyncio.run(upload_from_local.run(PAIRS_UPLOAD_MAX_FILE_CONCURRENCY))
        loop = asyncio.get_event_loop()
        loop.run_until_complete(upload_from_local.run(PAIRS_UPLOAD_MAX_FILE_CONCURRENCY))
        loop.close()
        n_retry += 1
    
    logger.info('Starting clean-up.')
    for g_file, m_file in zip(upload_files, meta_files):
        g_file.unlink()
        m_file.unlink()
    logger.info('Completed clean-up.')
########################################################################################################################    
## DAG definition
dag_default_args = {
    'owner' : 'Sebastien Fortier',
    'depends_on_past' : False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': pendulum.duration(hours=1),
}

with DAG(
    f'{dataset_short_name}',
    default_args=dag_default_args,
    description='ETL pipeline for 2017 HRDPS data',
    schedule_interval='@monthly',
    start_date=pendulum.datetime(2017, 1, 1, tz='UTC'),
    end_date=pendulum.datetime(2022, 6, 5, tz='UTC'),
    catchup=False,
    tags=['PAIRS', 'PAIRS ETL']
) as dag:
    
    task_start_etl_process = DummyOperator(task_id='start_etl_process')
    
   
    task_upload = PythonOperator(
        task_id='upload_to_pairs',
        max_active_tis_per_dag=PAIRS_UPLOAD_MAX_TASK_CONCURRENCY,
        python_callable=upload_to_pairs
    )
    
    task_end_etl_process = DummyOperator(task_id='end_etl_process')
    
    task_start_etl_process >> task_upload >> task_end_etl_process
# upload_to_pairs(pendulum.datetime(2017, 1, 1, tz='UTC'))
