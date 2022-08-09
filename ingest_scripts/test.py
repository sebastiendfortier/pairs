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

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup


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


global_pairsHost = 'https://pairsupl-ib:5011'

if not data_dir.is_dir():
    raise Exception(f'The data directory {data_dir} does not exist.')
if not upload_dir.is_dir():
    raise Exception(f'The upload directory {upload_dir} does not exist.')
    
# CDS Access
CDS_DOWNLOAD_MAX_CONCURRENCY=1

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

upload_credentials_file = Path('/home/pairs_loader/.pairs_upload_credentials')

dataset_id = '4'

layer_ids = {
 '4LFTX': 1,
 'ABSV': 2,
 'ALBDO': 3,
 'CAPE': 4,
 'CWAT': 5,
 'DEN': 6,
 'DEPR': 7,
 'DLWRF': 8,
 'DPT': 9,
 'DSWRF': 10,
 'GUST': 11,
 'HGT': 12,
 'HLCY': 13,
 'HPBL': 14,
 'ICEC': 15,
 'LAND': 16,
 'LHTFL': 17,
 'NLWRS': 18,
 'NSWRS': 19,
 'PRES': 20,
 'PRMSL': 21,
 'PTYPE': 22,
 'RH': 23,
 'SDEN': 24,
 'SDWE': 25,
 'SFCWRO': 26,
 'SHTFL': 27,
 'SHWINX': 28,
 'SKINT': 29,
 'SNOD': 30,
 'SOILM': 31,
 'SOILVIC': 32,
 'SPFH': 33,
 'TCDC': 34,
 'THICK': 35,
 'TMP': 36,
 'TSOIL': 37,
 'UGRD': 38,
 'ULWRF': 39,
 'USWRF': 40,
 'VGRD': 41,
 'VVEL': 42,
 'WDIR': 43,
 'WIND': 44,
 'WTMP': 45,
 'PRATE': 46,
 'APCP18': 47,
 'APCP': 48,
 'ACPCP': 49,
 'SPRATE': 50,
 'APCP24': 51,
 'APCP12': 52,
 'RPRATE': 53,
 'APCP48': 54,
 'IPRATE': 55,
 'FPRATE': 56,
 'APCP36': 57,
 'APCP30': 58,
 'APCP06': 59,
  }


######################
## Auxiliary functions
######################

def get_files_to_process(data_dir:Path, target_date:str, target_times:list) -> list:
    # Make a list of files to be ingested
    grib_files = glob.glob(str(data_dir / f'{target_date}*.grib2'))
    target_pos = len(str(data_dir)) + 9
    filtered_grib_files = np.sort([f for f in grib_files if f[target_pos:target_pos+6] in target_times])
    return filtered_grib_files

def get_layer_name(description:str, metadata:dict, projection:str, band_num:int) -> str:
    # extension = '.tif'
    level = copy.copy(description)
    type_of_level = copy.copy(description)
    level = re.sub('([0-9]+).*? .*?=.*$',r'\1',level)
    type_of_level = re.sub('.*? (.*?)=.*$',r'\1',type_of_level)
    if 'ISBL' == type_of_level:
        level = int(level)//100
        level = f'{level:04d}'

    layer_name = f"CMC_hrdps_continental_{metadata['GRIB_ELEMENT']}_{type_of_level}_{level}_{projection}_{datetime.datetime.fromtimestamp(int(metadata['GRIB_VALID_TIME'])).strftime('%Y%m%dT%H%M%SZ')}_P{int(metadata['GRIB_FORECAST_SECONDS'])//3600:03d}-00_{band_num}"
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
        dst_ds.SetProjection( srs.ExportToWkt() )        
        return dst_ds

def warp_to_wgs84(output_file_name:str, dst_ds:gdal.Dataset) -> gdal.Dataset:

    logger.info(f'Reprojecting {output_file_name}')

    warp_options = gdal.WarpOptions(dstSRS='EPSG:4326',format='GTiff',resampleAlg='near')#, creationOptions = ['TFW=YES', 'COMPRESS=LZW'])#,copyMetadata=True)
    
    warped_ds = gdal.Warp(output_file_name, dst_ds, outputType=gdal.GDT_Float32, options=warp_options)

    return warped_ds

def process_band(band_number:int, data_dir:Path, band:gdal.Band, dataset_info:dict, reproject:bool=False) -> gdal.Dataset:
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

        layer_name = get_layer_name(band_info['description'], band_info['metadata'], 'ps2.5km', band_number)

        u_file = upload_dir / f'{layer_name}.tiff'
        if os.path.isfile(u_file):
            return None, None, None

        # logger.info(str(u_file))
        logger.info(f'Processing {layer_name}')
        # output_file_name = str(data_dir / layer_name)

        dst_ds = create_dataset_from_band(driver, str(u_file), dataset_info, band_info)

        if reproject:
            layer_name = get_layer_name(band_info['description'], band_info['metadata'], 'EPSG_4326', band_number)
            u_file = upload_dir / f'{layer_name}.tiff'
            # output_file_name = str(data_dir / layer_name)
            warped_ds = warp_to_wgs84(str(u_file), band_info, dst_ds)
            return warped_ds

        m_file = u_file.with_suffix('.tiff.meta.json')
        url_str = 'http://pairsproxy.science.gc.ca/datasets/upload/' + str(os.path.basename(u_file))
        # logger.info(str(m_file))
        # logger.info(url_str)
        # logger.info({
        #             'datalayer_id' : [layer_ids[band_info['metadata']['GRIB_ELEMENT']]],
        #             'url' :  url_str,
        #             'pairsdatatype' : '2draster',
        #             'timestamp' : time_string,
        #             'ignoretile' : True,
        #             'datainterpolation' : 'near',
        #             'geospatialprojection' : 'EPSG:4326'
        #         })
        time_string = datetime.datetime.fromtimestamp(int(band_info['metadata']['GRIB_VALID_TIME'])).strftime('%Y%m%dT%H%M%SZ')
        with m_file.open('w') as fp:
            json.dump(
                {
                    'datalayer_id' : [layer_ids[band_info['metadata']['GRIB_ELEMENT']]],
                    'url' :  url_str,
                    'pairsdatatype' : '2draster',
                    'timestamp' : time_string,
                    'ignoretile' : True,
                    'datainterpolation' : 'near',
                    'geospatialprojection' : 'EPSG:4326'
                },
                fp
            )    

        return dst_ds, u_file, m_file
            
        
def process_grib_file(data_dir:Path, grib_file:str) -> None:
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
    upload_files = []
    meta_files = []
    for i in range(1,dataset.RasterCount+1):
        # get current band
        band = dataset.GetRasterBand(i)
        
        dst_ds, u_file, m_file = process_band(i, data_dir, band, dataset_info, reproject=False)
        if dst_ds is None:
            break

        upload_files.append(u_file)
        meta_files.append(m_file)

        # cleanup 
        del dst_ds    

        # cleanup
        del band

    # cleanup
    del dataset
    return upload_files, meta_files

# def write_geo_tiff(file_name: Union[str, Path], raster: np.array, swne: Sequence, no_data_value: Union[int, float] = None, dtype: str= None):
#     '''
#     Writes the raster in data['raster'] to a GeoTiff file.
#     Args:
#         file_name (str):    Name of the output file.
#         raster (np.array): The raster. (0, 0) is the top-left (north-west) pixel.
#                            The shape of the raster is (latitude, longitude).
#         swne (tuple):      Coordinates of bottom-left and top-right pixels.
#                            The convention is that these give the **centers** of
#                            the pixels.
#         no_data_value:       The no data value.
#         dtype (GDAL type): GDAL data type (e.g. Byte, UInt16, Int16, UInt32, Int32, Float32, Float64)
#     '''
#     try:
#         from osgeo import gdal
#     except ImportError:
#         logger.error('Unable to import \'gdal\'.')
#         raise
#     try:
#         import numpy as np
#     except ImportError:
#         logger.error('Unable to import \'numpy\'.')
#         raise
    
#     # We cannot set the default dtype in the function signature since we cannot 
#     # assume gdal to exist. So we set it here.
#     if dtype is None:
#         dtype = gdal.GDT_Float32
#     elif dtype == 'Byte':
#         dtype = gdal.GDT_Byte
#     elif dtype == 'UInt16':
#         dtype = gdal.GDT_UInt16
#     elif dtype == 'Int16':
#         dtype = gdal.GDT_Int16
#     elif dtype == 'UInt32':
#         dtype = gdal.GDT_UInt32
#     elif dtype == 'Int32':
#         dtype = gdal.GDT_Int32
#     elif dtype == 'Float32':
#         dtype = gdal.GDT_Float32
#     elif dtype == 'Float64':
#         dtype = gdal.GDT_Float64
#     else:
#         raise Exception(f'Data type {dtype} is not supported.')

#     n_j, n_i = raster.shape
#     if (n_j <= 1) or (n_i <= 1):
#         raise Exception('GeoTiff writer does not support rasters of a single pixel\'s width or heigth.')
#     lat_min, lon_min, lat_max, lon_max = swne
#     d_lat = (lat_max - lat_min) / (n_j - 1)
#     d_lon = (lon_max - lon_min) / (n_i - 1)
#     nw_corner_lat = lat_max + d_lat / 2.
#     nw_corner_lon = lon_min - d_lon / 2.
#     gdal_geo_ref = (nw_corner_lon, d_lon, 0, nw_corner_lat, 0, -d_lat)
#     if no_data_value is not None:
#         raster = np.where(np.isnan(raster), no_data_value, raster)

#     # Use GDAL to create the GeoTiff
#     drv = gdal.GetDriverByName('GTiff')
#     ds = drv.Create(str(file_name), n_i, n_j, 1, dtype)
    
#     from osgeo import osr
#     sr = osr.SpatialReference()
#     sr.ImportFromEPSG(4326)
#     ds.SetProjection(sr.ExportToWkt())

#     ds.SetGeoTransform(gdal_geo_ref)
#     raster_band = ds.GetRasterBand(1)
#     if no_data_value is not None:
#         raster_band.SetNoDataValue(no_data_value)
#     raster_band.WriteArray(raster)
#     ds = None

## Auxiliary functions - Climate Data Store
# def build_cds_request(parameter: str, year: int, month: int) -> Tuple[str, Dict]:
#     '''
#     Build a CDS request
#     '''
#     calendar = Calendar()
#     days_in_month = [d for d in calendar.itermonthdates(year, month) if d.month==month]
    
#     cds_request = {
#         'format': 'netcdf',
#         'variable': [parameter],
#         'year': f'{year}',
#         'month': f'{month:0>2}',
#         'day': [f'{d.day:0>2}' for d in days_in_month],
#         'time': [f'{h:0>2}:00' for h in range(24)],
#         # Area is defined NWSE
#         'area' : [
#             spatial_coverage['lat'][1],
#             spatial_coverage['lon'][0],
#             spatial_coverage['lat'][0],
#             spatial_coverage['lon'][1]
#         ]
#     }
    
#     file_name = f'era5_land_{parameter}_{year}{month:0>2}.nc'
    
#     return file_name, cds_request

# def get_cds_data(dataset_short_name: str, request: Dict, target_file_name: str) -> None:
#     download_file = download_dir / target_file_name
    
#     cds_client = cdsapi.Client()
#     try:
#         cds_request = cds_client.retrieve(dataset_short_name, request)
#     except Exception as e:
#         logger.error('CDS request failed.')
#         raise e
#     try:
#         cds_request.download(str(download_file))
#     except Exception as e:
#         logger.error('CDS download failed.')
#         raise e
#     else:
#         move(str(download_file), str(archive_dir))

# def download_from_cds(parameter: str, data_interval_start: pendulum.datetime, ti:TaskInstance) -> None:
#     """
#     #### Data extraction from the CDS
#     """

#     logger.debug(
#         f'Download_from_cds - parameter: {parameter}, data_interval_start: {data_interval_start}.'
#     )
    
#     data_info = None

#     file_name, cds_request = build_cds_request(
#         parameter, data_interval_start.year, data_interval_start.month
#     )
    
#     if (download_dir / file_name).is_file():
#         logger.error(f'Found {download_dir / file_name}.')
#         raise Exception(f'File {file_name} exists in download directory; seems that download is in progress.')

#     if (archive_dir / file_name).is_file():
#         logger.info(f'Found {archive_dir / file_name}.')
#         data_info = (p, str(archive_dir/file_name))
#     else:
#         logger.info(f'Will request {archive_dir / file_name} from CDS.')
#         get_cds_data(dataset_short_name, cds_request, file_name)
#         logger.info(f'Obtained {archive_dir / file_name} from CDS.')
#         data_info = (p, str(archive_dir/file_name))

#     ti.xcom_push(key='data_file', value=str(archive_dir/file_name))
#     ti.xcom_push(key='parameter', value=parameter)

def upload_to_pairs(data_interval_start: pendulum.datetime):#, ti: TaskInstance) -> None:
    # data_parameters = ti.xcom_pull(
    #     key='parameter',
    #     task_ids=[f'download_from_cds.download_{p}' for p in parameters]
    # )
    # data_files = ti.xcom_pull(
    #     key='data_file',
    #     task_ids=[f'download_from_cds.download_{p}' for p in parameters]
    # )
    # base_path = Path('/fs/site6/eccc/cmd/w/sbf000/operation.forecasts.lamgrib2.hrdps_national.continental/')

    target_date = '2017'

    targets_24h = ['00_000','00_024','00_048','06_018','06_42','12_012','12_036','18_006','18_030']

    filtered_grib_files = get_files_to_process(data_dir, target_date, targets_24h)

    upload_files = []
    meta_files = []

    for grib_file in filtered_grib_files:
        upload_files, meta_files = process_grib_file(data_dir, grib_file)
        
        if len(upload_files) == 0:
            continue
        # logger.info(upload_files)
        # logger.info(meta_files)
#     for d_file in data_files:
#         with xr.open_dataarray(d_file) as xr_array:
#             for t in xr_array.coords['time']:
#                 time_string = t.values.astype('datetime64[s]').astype(datetime.datetime).strftime('%Y%m%dT%H%M%SZ')
#                 #u_file = upload_dir / f'era5_raster_{xr_array.name}_{time_string}.nc'
#                 #xr_array.loc[t].to_netcdf(u_file)
#                 u_file = upload_dir / f'era5_raster_{xr_array.name}_{time_string}.tiff'
#                 swne = (
#                     np.min(xr_array['latitude'].values),
#                     np.min(xr_array['longitude'].values),
#                     np.max(xr_array['latitude'].values),
#                     np.max(xr_array['longitude'].values)
#                 )
#                 write_geo_tiff(u_file, xr_array.loc[t].values, swne, dtype='Float32')
#                 logger.debug(f'Created {u_file}.')
# ##                 g_file = u_file.with_suffix('.gz')
# ##                 with u_file.open('rb') as f_in:
# ##                     with gzip.open(g_file, 'wb') as f_out:
# ##                         copyfileobj(f_in, f_out)
# ##                 u_file.unlink()
                
#                 m_file = u_file.with_suffix('.tiff.meta.json')
#                 url_str = 'http://pairsproxy.science.gc.ca/datasets/upload/' + str(os.path.basename(u_file))
#                 with m_file.open('w') as fp:
#                     json.dump(
#                         {
#                             'datalayer_id' : [layer_ids[metadata['GRIB_ELEMENT']]],
#                             'url' :  url_str,
#                             'pairsdatatype' : '2draster',
#                             'timestamp' : time_string,
#                             'ignoretile' : True,
#                             'datainterpolation' : 'near',
#                             'geospatialprojection' : 'EPSG:4326'
#                         },
#                         fp
#                     )
#                 logger.debug(f'Created {m_file}.')
                    
#                 upload_files.append(u_file)
#                 meta_files.append(m_file)
                
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
    description='ETL pipeline for HRDPS data',
    schedule_interval='@monthly',
    start_date=pendulum.datetime(2017, 1, 1, tz='UTC'),
    end_date=pendulum.datetime(2017, 12, 31, tz='UTC'),
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

