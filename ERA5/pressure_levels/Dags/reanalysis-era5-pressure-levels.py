import json
import pendulum, datetime
from pathlib import Path
from calendar import Calendar
from shutil import move, copyfileobj
import gzip
import logging
import cdsapi
import re
import xarray as xr, numpy as np
from ibmpairs import uploads
import asyncio

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import uuid
from dateutil.parser import parse as date_parser
import pytz

epochTimeZero = datetime.datetime(1970,1,1,0,0,0,0,tzinfo=pytz.utc)
from typing import Tuple, Dict, List, Sequence, Union
import os

logger = logging.getLogger()

################
## Configuration
################

# ETL test mode
PAIRS_ETL_TEST=True

# Directories
## data_dir = Path('/home/johannes/Pairs/PairsData/Datasets/ERA5Land/Data/')
## archive_dir = data_dir / 'Archive'
## download_dir = data_dir / 'Download'
## upload_dir = data_dir / 'Upload'
data_dir = Path('/pairs_gpfs/datasets/pairs-etl-data/era5-pressure/')
archive_dir = data_dir / 'archive'
download_dir = data_dir / 'download'
## upload_dir = data_dir / 'upload'
#upload_dir = Path('/pairs_gpfs/datasets/upload/')
upload_dir = Path('/data/pairs-uploader/datasets/era5-pressure/input/')

if not upload_dir.is_dir():
    os.makedirs(upload_dir)

global_pairsHost = 'https://pairsupl-ib:5011'

if not archive_dir.is_dir():
    raise Exception(f'The archive directory {archive_dir} does not exist.')
if not download_dir.is_dir():
    raise Exception(f'The download directory {download_dir} does not exist.')
if not upload_dir.is_dir():
    raise Exception(f'The upload directory {upload_dir} does not exist.')
    
# CDS Access
CDS_DOWNLOAD_MAX_CONCURRENCY=1

# Product definition
spatial_coverage = {
    'lat' : (0, 85),    #(40, 85)
    'lon' : (-160, -30) #(-143, -51)
}

dataset_short_name = 'reanalysis-era5-pressure-levels'

parameters =  [
            'geopotential', 
            'temperature', 
            'u_component_of_wind',
            'v_component_of_wind'
            # 'vertical_velocity', 
            # 'vorticity',
        ]

# pressure_level =  [
#             '50', '100', '150',
#             '175', '200', '225',
#             '250', '300', '350',
#             '400', '450', '500',
#             '550', '600', '650',
#             '700', '750', '800',
#             '850', '875', '900',
#             '925', '950', '975',
#             '1000',
#         ]

pressure_level =  [
            '300', '400', '500', '600', '700', '800', '900','1000'
        ]
# PAIRS upload
PAIRS_UPLOAD_MAX_TASK_CONCURRENCY=1
PAIRS_UPLOAD_MAX_FILE_CONCURRENCY=10
PAIRS_UPLOAD_MAX_RETRIES=2

upload_credentials_file = Path('/home/pairs_loader/.pairs_upload_credentials_upladm')

dataset_id = '13'
dataset_key = 'era5-pressure-levels'


layer_ids = {
'z': 'geopotential',
't': 'temperature',
'u': 'u-component-of-wind',
'v': 'v-component-of-wind',
'w': 'vertical-velocity',
'vo': 'vorticity-relative'
}

######################
## Auxiliary functions
######################

def write_geo_tiff(file_name: Union[str, Path], raster: np.array, swne: Sequence, no_data_value: Union[int, float] = None, dtype: str= None):
    '''
    Writes the raster in data['raster'] to a GeoTiff file.
    Args:
        file_name (str):    Name of the output file.
        raster (np.array): The raster. (0, 0) is the top-left (north-west) pixel.
                           The shape of the raster is (latitude, longitude).
        swne (tuple):      Coordinates of bottom-left and top-right pixels.
                           The convention is that these give the **centers** of
                           the pixels.
        no_data_value:       The no data value.
        dtype (GDAL type): GDAL data type (e.g. Byte, UInt16, Int16, UInt32, Int32, Float32, Float64)
    '''
    try:
        from osgeo import gdal
    except ImportError:
        logger.error('Unable to import \'gdal\'.')
        raise
    try:
        import numpy as np
    except ImportError:
        logger.error('Unable to import \'numpy\'.')
        raise
    
    # We cannot set the default dtype in the function signature since we cannot 
    # assume gdal to exist. So we set it here.
    if dtype is None:
        dtype = gdal.GDT_Float32
    elif dtype == 'Byte':
        dtype = gdal.GDT_Byte
    elif dtype == 'UInt16':
        dtype = gdal.GDT_UInt16
    elif dtype == 'Int16':
        dtype = gdal.GDT_Int16
    elif dtype == 'UInt32':
        dtype = gdal.GDT_UInt32
    elif dtype == 'Int32':
        dtype = gdal.GDT_Int32
    elif dtype == 'Float32':
        dtype = gdal.GDT_Float32
    elif dtype == 'Float64':
        dtype = gdal.GDT_Float64
    else:
        raise Exception(f'Data type {dtype} is not supported.')

    n_j, n_i = raster.shape
    if (n_j <= 1) or (n_i <= 1):
        raise Exception('GeoTiff writer does not support rasters of a single pixel\'s width or heigth.')
    lat_min, lon_min, lat_max, lon_max = swne
    d_lat = (lat_max - lat_min) / (n_j - 1)
    d_lon = (lon_max - lon_min) / (n_i - 1)
    nw_corner_lat = lat_max + d_lat / 2.
    nw_corner_lon = lon_min - d_lon / 2.
    gdal_geo_ref = (nw_corner_lon, d_lon, 0, nw_corner_lat, 0, -d_lat)
    if no_data_value is not None:
        raster = np.where(np.isnan(raster), no_data_value, raster)

    # Use GDAL to create the GeoTiff
    drv = gdal.GetDriverByName('GTiff')
    ds = drv.Create(str(file_name), n_i, n_j, 1, dtype)
    
    from osgeo import osr
    sr = osr.SpatialReference()
    sr.ImportFromEPSG(4326)
    ds.SetProjection(sr.ExportToWkt())

    ds.SetGeoTransform(gdal_geo_ref)
    raster_band = ds.GetRasterBand(1)
    if no_data_value is not None:
        raster_band.SetNoDataValue(no_data_value)
    raster_band.WriteArray(raster)
    ds = None

## Auxiliary functions - Climate Data Store
def build_cds_request(parameter: str, level: str, year: int, month: int) -> Tuple[str, Dict]:
    '''
    Build a CDS request
    '''
    calendar = Calendar()
    days_in_month = [d for d in calendar.itermonthdates(year, month) if d.month==month]
    # c.retrieve(
    # 'reanalysis-era5-pressure-levels',
    # {
    #     'product_type': 'reanalysis',
    #     'format': 'netcdf',
    #     'variable': 'temperature',
    #     'pressure_level': '2',
    #     'year': '2017',
    #     'month': '01',
    #     'day': [
    #         '01', '02',
    #     ],
    #     'time': '00:00',
    # },
    # 'download.nc')

    cds_request = {
        'product_type': 'reanalysis',
        'format': 'netcdf',
        'variable': [parameter],
        'pressure_level': [level],
        'year': f'{year}',
        'month': f'{month:0>2}',
        'day': [f'{d.day:0>2}' for d in days_in_month],
        'time': [f'{h:0>2}:00' for h in range(0,24,3)], # mod range was -> range(24)
        # Area is defined NWSE
        'area' : [
            spatial_coverage['lat'][1],
            spatial_coverage['lon'][0],
            spatial_coverage['lat'][0],
            spatial_coverage['lon'][1]
        ]
    }

    file_name = f'era5_pressure_{parameter}_{year}{month:0>2}_{level}.nc'
    
    return file_name, cds_request

def get_cds_data(dataset_short_name: str, request: Dict, target_file_name: str) -> None:
    download_file = download_dir / target_file_name
    
    cds_client = cdsapi.Client()
    try:
        logger.info(f'{request}')
        cds_request = cds_client.retrieve(dataset_short_name, request)
    except Exception as e:
        logger.error('CDS request failed.')
        raise e
    try:
        cds_request.download(str(download_file))
    except Exception as e:
        logger.error('CDS download failed.')
        raise e
    else:
        move(str(download_file), str(archive_dir))

def download_from_cds(parameter: str, level: str, data_interval_start: pendulum.datetime, ti:TaskInstance) -> None:
    """
    #### Data extraction from the CDS
    """

    logger.debug(
        f'Download_from_cds - parameter: {parameter}, level: {level}, data_interval_start: {data_interval_start}.'
    )
    
    data_info = None

    file_name, cds_request = build_cds_request(
        parameter, level, data_interval_start.year, data_interval_start.month
    )
    
    logger.info('current csd request')
    logger.info(cds_request)

    if (download_dir / file_name).is_file():
        logger.error(f'Found {download_dir / file_name}.')
        raise Exception(f'File {file_name} exists in download directory; seems that download is in progress.')

    if (archive_dir / file_name).is_file():
        logger.info(f'Found {archive_dir / file_name}.')
        data_info = (p, str(archive_dir/file_name))
    else:
        logger.info(f'Will request {archive_dir / file_name} from CDS.')
        get_cds_data(dataset_short_name, cds_request, file_name)
        logger.info(f'Obtained {archive_dir / file_name} from CDS.')
        data_info = (p, str(archive_dir/file_name))

    ti.xcom_push(key='data_file', value=str(archive_dir/file_name))
    ti.xcom_push(key='parameter', value=parameter)
    ti.xcom_push(key='level', value=level)

def upload_to_pairs(data_interval_start: pendulum.datetime, ti: TaskInstance) -> None:
    logger.info(f'{__file__} last modified on {datetime.datetime.fromtimestamp(os.path.getmtime(__file__)).replace(microsecond=0)}')
    logger.info(f'Current working directory is {os.getcwd()}')
    data_parameters = ti.xcom_pull(
        key='parameter',
        task_ids=[f'download_from_cds.download_{p}_{l}' for p in parameters for l in pressure_level]
    )
    data_files = ti.xcom_pull(
        key='data_file',
        task_ids=[f'download_from_cds.download_{p}_{l}' for p in parameters for l in pressure_level]
    )
    
    upload_files = []
    meta_files = []
    
    for d_file in data_files:
        with xr.open_dataarray(d_file) as xr_array:
            logger.info(f'procesing: {d_file}')
            level = re.sub('.*_(.*?).nc',r'\1',d_file)
            logger.info(f'xarray: {xr_array}')
            for t in xr_array.coords['time']:
                if xr_array.name in layer_ids.keys():
                    layer_key = layer_ids[xr_array.name]
                else:
                    logger.warning(f'{xr_array.name} not found in {layer_ids.keys()} - skipping')
                    break
                
                # level = xr_array.coords['level'].data
                time_string = t.values.astype('datetime64[s]').astype(datetime.datetime).strftime('%Y%m%dT%H%M%SZ')
                timestamp1 = date_parser(time_string)
                timestamp1 = timestamp1.replace(tzinfo=pytz.UTC)
                time_epoch = int ((timestamp1 - epochTimeZero).total_seconds()) 
                #u_file = upload_dir / f'era5_raster_{xr_array.name}_{time_string}.nc'
                #xr_array.loc[t].to_netcdf(u_file)
                u_file = upload_dir / f'era5_pressure_{xr_array.name}_{time_string}_{level}.tiff'
                swne = (
                    np.min(xr_array['latitude'].values),
                    np.min(xr_array['longitude'].values),
                    np.max(xr_array['latitude'].values),
                    np.max(xr_array['longitude'].values)
                )
                write_geo_tiff(u_file, xr_array.loc[t].values, swne, dtype='Float32')
                logger.debug(f'Created {u_file}.')
##                 g_file = u_file.with_suffix('.gz')
##                 with u_file.open('rb') as f_in:
##                     with gzip.open(g_file, 'wb') as f_out:
##                         copyfileobj(f_in, f_out)
##                 u_file.unlink()
                
                # m_file = u_file.with_suffix('.tiff.meta.json')
                m_file = u_file.with_suffix('.tiff.meta')
                url_str = 'http://pairsproxy.science.gc.ca/datasets/upload/' + str(os.path.basename(u_file))
                # with m_file.open('w') as fp:
                #     json.dump(
                #         {
                #             'datalayer_id' : [layer_ids[xr_array.name]],
                #             'url' :  url_str,
                #             'pairsdatatype' : '2draster',
                #             'timestamp' : time_string,
                #             'ignoretile' : True,
                #             'datainterpolation' : 'near',
                #             'geospatialprojection' : 'EPSG:4326'
                #         },
                #         fp
                #     )
                
                with m_file.open('w') as fp:
                    fp.write("pairsdataset=" + dataset_key + "\n")
                    fp.write("pairsdatalayer=" + layer_key + "\n")
                    fp.write("timestamp=" + str(int(time_epoch)) + "\n")
                    fp.write("pairsdatatype=2draster"+ "\n")
                    fp.write("geospatialprojection=EPSG:4326"+ "\n")
                    fp.write(f"pairsdimension=level\n")
                    fp.write(f"dimension_value={level}\n")
                    fp.write("band=1")    
                logger.debug(f'Created {m_file}.')
                    
                upload_files.append(u_file)
                meta_files.append(m_file)
                
    logger.info(f'Generated {len(upload_files)} raster and {len(meta_files)} meta files.')
                
#     with upload_credentials_file.open() as fp:
#         upload_credentials = json.load(fp)
        
# ##     logger.info('Connecting to COS bucket.')
# ##     cos_bucket = uploads.IBMCosBucket(
# ##         upload_credentials['apikey'], upload_credentials['resourceInstanceID'],
# ##         upload_credentials['authEndpoint'], upload_credentials['endpoint'],
# ##         upload_credentials['bucketName'], upload_credentials['accessKeyID'], upload_credentials['secretAccessKey']
# ##     )
#     pairs_auth = (upload_credentials['pairsUser'], upload_credentials['pairsPassword'])
    
#     upload_from_local = uploads.PAIRSUpload.fromLocal(
#         pairs_auth, fileList=upload_files,pairsHost=global_pairsHost
#     )
#     logger.info('Commencing PAIRS upload.')
#     #asyncio.run(upload_from_local.run(PAIRS_UPLOAD_MAX_FILE_CONCURRENCY))
#     loop = asyncio.get_event_loop()
#     loop.run_until_complete(upload_from_local.run(PAIRS_UPLOAD_MAX_FILE_CONCURRENCY))
#     loop.close()
    
#     n_retry = 1
#     failed_uploads = {u.localFile for u in upload_from_local.completedUploads if not u.status == 'SUCCEEDED'}
#     while (n_retry <= PAIRS_UPLOAD_MAX_RETRIES) and len(failed_uploads) > 0:
#         upload_from_local = uploads.PAIRSUpload.fromLocal(
#             pairs_auth, fileList=failed_uploads,pairsHost=global_pairsHost
#         )
#         logger.warning(f'{len(failed_uploads)} files failed uploading. Commencing attempt {n_retry}/{PAIRS_UPLOAD_MAX_FILE_CONCURRENCY}.')
#         #asyncio.run(upload_from_local.run(PAIRS_UPLOAD_MAX_FILE_CONCURRENCY))
#         loop = asyncio.get_event_loop()
#         loop.run_until_complete(upload_from_local.run(PAIRS_UPLOAD_MAX_FILE_CONCURRENCY))
#         loop.close()
#         n_retry += 1
    
    # logger.info('Starting clean-up.')
    # for g_file, m_file in zip(upload_files, meta_files):
    #     g_file.unlink()
    #     m_file.unlink()
    # logger.info('Completed clean-up.')
    
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
    'pairs_etl_era5_pressure_levels_v1',
    default_args=dag_default_args,
    description='ETL pipeline for ERA5 pressure levels data',
    schedule_interval='@monthly',
    start_date=pendulum.datetime(2000, 1, 1, tz='UTC'),
    end_date=None,
    catchup=False,
    tags=['PAIRS', 'PAIRS ETL']
) as dag:
    
    task_start_etl_process = DummyOperator(task_id='start_etl_process')
    
    with TaskGroup(group_id='download_from_cds') as task_group_download:
        download_tasks = []
        
        for p in parameters:
            for l in pressure_level:
                task_download = PythonOperator(
                    task_id=f'download_{p}_{l}',
                    do_xcom_push=False,
                    max_active_tis_per_dag=CDS_DOWNLOAD_MAX_CONCURRENCY,
                    python_callable=download_from_cds,
                    op_kwargs={'parameter':p, 'level':l}
                )
                download_tasks.append(task_download)
    
    task_upload = PythonOperator(
        task_id='upload_to_pairs',
        max_active_tis_per_dag=PAIRS_UPLOAD_MAX_TASK_CONCURRENCY,
        python_callable=upload_to_pairs
    )
    
    task_end_etl_process = DummyOperator(task_id='end_etl_process')
    
    task_start_etl_process >> task_group_download >> task_upload >> task_end_etl_process
