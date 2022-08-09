#!/bin/env python3
from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from calendar import Calendar
from dateutil.parser import parse as date_parser
from ibmpairs import uploads
from pathlib import Path
from shutil import move
from typing import Tuple, Dict, List, Sequence, Union
import cdsapi
import logging
import os
import pendulum, datetime
import pytz
import xarray as xr, numpy as np


epochTimeZero = datetime.datetime(1970,1,1,0,0,0,0,tzinfo=pytz.utc)

logger = logging.getLogger()

################
## Configuration
################

# ETL test mode
PAIRS_ETL_TEST=False

def define_dir(path: Path):
    if not path.is_dir():
        logger.info(f'creating directory {str(path)}')
        os.makedirs(str(path))
    return path

# Directories

data_dir = define_dir(Path('/pairs_gpfs/datasets/pairs-etl-data/era5-single/'))
archive_dir = define_dir(data_dir / 'archive')
download_dir = define_dir(data_dir / 'download')

global_pairsHost = 'https://pairsupl-ib:5011'

upload_dir = Path('/data/pairs-uploader/datasets/era5-single/input/')

if not upload_dir.is_dir():
    os.makedirs(upload_dir)
    
# CDS Access
CDS_DOWNLOAD_MAX_CONCURRENCY=1

# Product definition
spatial_coverage = {
    'lat' : (0, 85),    #(40, 85)
    'lon' : (-160, -30) #(-143, -51)
}

dataset_short_name = 'reanalysis-era5-single-levels'
parameters = [
    '10m_u_component_of_wind', 
    '10m_v_component_of_wind', 
    '10m_wind_gust_since_previous_post_processing',
    '2m_dewpoint_temperature', 
    '2m_temperature', 'geopotential',
    'maximum_individual_wave_height', 
    'mean_sea_level_pressure', 
    'precipitation_type',
    'sea_ice_cover', 
    'significant_height_of_combined_wind_waves_and_swell', 
    'significant_height_of_wind_waves',
    'snowfall', 
    'surface_pressure', 
    'surface_solar_radiation_downwards',
    'toa_incident_solar_radiation', 
    'total_cloud_cover', 
    'total_column_water_vapour',
    'total_precipitation', 
    'vertical_integral_of_eastward_water_vapour_flux', 
    'vertical_integral_of_northward_water_vapour_flux',
]

# PAIRS upload
PAIRS_UPLOAD_MAX_TASK_CONCURRENCY=1
PAIRS_UPLOAD_MAX_FILE_CONCURRENCY=10
PAIRS_UPLOAD_MAX_RETRIES=2

upload_credentials_file = Path('/home/pairs_loader/.pairs_upload_credentials_upladm')

dataset_id = '14'
dataset_key = 'era5-single-levels'

layer_ids = {
 'u10': 'u-wind-component-10m',
 'v10': 'v-wind-component-10m',
 'fg10': 'wind-gust-since-previous-post-processing-10m',
 'd2m': 'dewpoint-temperature-2m',
 't2m': 'temperature-2m',
 'z': 'geopotential',
 'hmax': 'maximum-individual-wave-height',
 'msl': 'mean-sea-level-pressure',
 'ptype': 'precipitation-type',
 'siconc': 'sea-ice-area-fraction',
 'swh': 'significant-height-of-combined-wnd-wves-and-swll',
 'shww': 'significant-height-of-wind-waves',
 'sf': 'snowfall',
 'sp': 'surface-pressure',
 'ssrd': 'surface-solar-radiation-downwards',
 'tisr': 'toa-incident-solar-radiation',
 'tcc': 'total-cloud-cover',
 'tcwv': 'total-column-vertically-integrated-water-vapour',
 'tp': 'total-precipitation',
 'p71.162': 'vertical-integral-of-eastward-water-vapour-flux',
 'p72.162': 'vertical-integral-of-northward-water-vapour-flux'
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
def build_cds_request(parameter: str, year: int, month: int) -> Tuple[str, Dict]:
    '''
    Build a CDS request
    '''
    calendar = Calendar()
    days_in_month = [d for d in calendar.itermonthdates(year, month) if d.month==month]
    
    cds_request = {
        'product_type': 'reanalysis',
        'format': 'netcdf',
        'variable': [parameter],
        'year': f'{year}',
        'month': f'{month:0>2}',
        'day': [f'{d.day:0>2}' for d in days_in_month],
        'time': [f'{h:0>2}:00' for h in range(24)],
        # Area is defined NWSE
        'area' : [
            spatial_coverage['lat'][1],
            spatial_coverage['lon'][0],
            spatial_coverage['lat'][0],
            spatial_coverage['lon'][1]
        ]
    }

    file_name = f'era5_single_{parameter}_{year}{month:0>2}.nc'
    
    return file_name, cds_request

def get_cds_data(dataset_short_name: str, request: Dict, target_file_name: str) -> None:
    download_file = download_dir / target_file_name
    
    cds_client = cdsapi.Client()
    try:
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

def download_from_cds(parameter: str, data_interval_start: pendulum.datetime, ti:TaskInstance) -> None:
    """
    #### Data extraction from the CDS
    """

    logger.debug(
        f'Download_from_cds - parameter: {parameter}, data_interval_start: {data_interval_start}.'
    )
    
    data_info = None

    file_name, cds_request = build_cds_request(
        parameter, data_interval_start.year, data_interval_start.month
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

def upload_to_pairs(data_interval_start: pendulum.datetime, ti: TaskInstance) -> None:
    logger.info(f'{__file__} last modified on {datetime.datetime.fromtimestamp(os.path.getmtime(__file__)).replace(microsecond=0)}')
    logger.info(f'Current working directory is {os.getcwd()}')
    # upload_dir = Path(f'/data/pairs-uploader/pre-uploader_datasets/ERA5_single.{data_interval_start.year}{data_interval_start.month:02}/input/')
    # if not os.path.exists(upload_dir):
    #     os.makedirs(upload_dir)

    logger.info(f'upload_dir = {upload_dir}')

    data_parameters = ti.xcom_pull(
        key='parameter',
        task_ids=[f'download_from_cds.download_{p}' for p in parameters]
    )
    data_files = ti.xcom_pull(
        key='data_file',
        task_ids=[f'download_from_cds.download_{p}' for p in parameters]
    )
    
    upload_files = []
    meta_files = []
    
    for d_file in data_files:
        with xr.open_dataarray(d_file) as xr_array:
            for t in xr_array.coords['time']:
                if xr_array.name in layer_ids.keys():
                    layer_key = layer_ids[xr_array.name]
                else:
                    logger.warning(f'{xr_array.name} not found in {layer_ids.keys()} - skipping')
                    break
                time_string = t.values.astype('datetime64[s]').astype(datetime.datetime).strftime('%Y%m%dT%H%M%SZ')
                timestamp1 = date_parser(time_string)
                timestamp1 = timestamp1.replace(tzinfo=pytz.UTC)
                time_epoch = int ((timestamp1 - epochTimeZero).total_seconds()) 
                #u_file = upload_dir / f'era5_raster_{xr_array.name}_{time_string}.nc'
                #xr_array.loc[t].to_netcdf(u_file)
                u_file = upload_dir / f'era5_raster_{xr_array.name}_{time_string}.tiff'
                swne = (
                    np.min(xr_array['latitude'].values),
                    np.min(xr_array['longitude'].values),
                    np.max(xr_array['latitude'].values),
                    np.max(xr_array['longitude'].values)
                )
                write_geo_tiff(u_file, xr_array.loc[t].values, swne, dtype='Float32')
                logger.debug(f'Created {u_file}.')

                m_file = u_file.with_suffix('.tiff.meta')
                # url_str = 'http://pairsproxy.science.gc.ca/datasets/upload/' + str(os.path.basename(u_file))

                
                with m_file.open('w') as fp:
                    fp.write("pairsdataset=" + dataset_key + "\n")
                    fp.write("pairsdatalayer=" + layer_key + "\n")
                    fp.write("timestamp=" + str(int(time_epoch)) + "\n")
                    fp.write("pairsdatatype=2draster"+ "\n")
                    fp.write("geospatialprojection=EPSG:4326"+ "\n")
                    fp.write("band=1")    
                logger.debug(f'Created {m_file}.')
                    
                upload_files.append(u_file)
                meta_files.append(m_file)
                
    logger.info(f'Generated {len(upload_files)} raster and {len(meta_files)} meta files.')
    
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
    'pairs_etl_era5_single_levels_v1',
    default_args=dag_default_args,
    description='ETL pipeline for ERA5 single levels data',
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
            task_download = PythonOperator(
                task_id=f'download_{p}',
                do_xcom_push=False,
                max_active_tis_per_dag=CDS_DOWNLOAD_MAX_CONCURRENCY,
                python_callable=download_from_cds,
                op_kwargs={'parameter':p}
            )
            download_tasks.append(task_download)
    
    task_upload = PythonOperator(
        task_id='upload_to_pairs',
        max_active_tis_per_dag=PAIRS_UPLOAD_MAX_TASK_CONCURRENCY,
        python_callable=upload_to_pairs
    )
    
    task_end_etl_process = DummyOperator(task_id='end_etl_process')
    
    task_start_etl_process >> task_group_download >> task_upload >> task_end_etl_process
