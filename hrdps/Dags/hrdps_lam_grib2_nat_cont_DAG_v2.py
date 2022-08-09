#!/bin/env python3
from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import timezone
from ibmpairs import uploads
from itertools import repeat
from osgeo import gdal, osr
from pathlib import Path
import copy
import glob
import logging
import multiprocessing
import numpy as np
import os
import pendulum, datetime
import re
import uuid


datasets = {
# 'DBLL':'hrdps-lam-nat-cont-dbll',
'EATM':'hrdps-lam-nat-cont-eatm',
'EtaL':'hrdps-lam-nat-cont-etal',
'HTGL':'hrdps-lam-nat-cont-htgl',
'ISBL':'hrdps-lam-nat-cont-isbl',
'MSL':'hrdps-lam-nat-cont-msl',
# 'NTAT':'hrdps-lam-nat-cont-ntat',
'SFC':'hrdps-lam-nat-cont-sfc'
}

layers = {
'hrdps-lam-nat-cont-ntat': {
    'DSWRF': 'downward-short-wave-radiation-flux'
    # 'ULWRF': 'upward-long-wave-radiation-flux',
    # 'USWRF': 'upward-short-wave-radiation-flux'
    },
'hrdps-lam-nat-cont-eatm': {'CWAT': 'cloud-water'}, 
# 'hrdps-lam-nat-cont-dbll': {'SOILM': 'soil-moisture-content', 'TSOIL': 'soil-temperature'},
'hrdps-lam-nat-cont-isbl': {
    # 'ABSV': 'absolute-vorticity',
    # '4LFTX': 'best-4-layer-lifted-index',
    # 'DEPR': 'dew-point-depression',
    'HGT': 'geopotential-height',
    # 'RH': 'relative-humidity',
    'SHWINX': 'showalter-index',
    'SPFH': 'specific-humidity',
    'TMP': 'temperature',
    # 'THICK': 'thickness',
    # 'VVEL': 'vertical-velocity--pressure-',
    'WDIR': 'wind-direction-from-which-blowing',
    'WIND': 'wind-speed',
    # 'UGRD': 'u-component-of-wind',
    # 'VGRD': 'v-component-of-wind'
    }, 
'hrdps-lam-nat-cont-msl': {'PRMSL': 'pressure-reduced-to-msl'},
    'hrdps-lam-nat-cont-sfc': {
        'APCP06': 'total-precipitation-06-hr',
        'APCP12': 'total-precipitation-12-hr',
        'APCP18': 'total-precipitation-18-hr',
        'APCP24': 'total-precipitation-24-hr',
        'APCP30': 'total-precipitation-30-hr',
        'APCP36': 'total-precipitation-36-hr',
        'APCP42': '42-hr-total-precipitation',
        'APCP48': '48-hr-total-precipitation',
        # 'ALBDO': 'albedo',
        'ACPCP': 'convective-precipitation',
        'DLWRF': 'downward-long-wave-radiation-flux',
        'DSWRF': 'downward-short-wave-radiation-flux',
        # 'FPRATE': 'freezing-rain-precipitation-rate',
        # 'HGT': 'geopotential-height',
        'ICEC': 'ice-cover',
        # 'IPRATE': 'ice-pellets-precipitation-rate',
        # 'LAND': 'land-cover',
        # 'LHTFL': 'latent-heat-net-flux',
        'NLWRS': 'net-long-wave-radiation-flux-surface',
        'NSWRS': 'net-short-wave-radiation-flux-surface',
        # 'HPBL': 'planetary-boundary-layer-height',
        'PRATE': 'precipitation-rate',
        'PTYPE': 'precipitation-type',
        'PRES': 'pressure',
        'RPRATE': 'rain-precipitation-rate',
        # 'SHTFL': 'sensible-heat-net-flux',
        # 'SKINT': 'skin-temperature',
        'SDEN': 'snow-density',
        'SNOD': 'snow-depth',
        'SDWE': 'snow-depth-water-equivalent',
        'SPRATE': 'snow-precipitation-rate',
        # 'SOILVIC': 'soil-volumetric-ice-content-water-equivalent',
        # 'TSOIL': 'soil-temperature',
        # 'SPFH': 'specific-humidity',
        # 'SFCWRO': 'surface-water-runoff',
        'TCDC': 'total-cloud-cover'
        # 'WTMP': 'water-temperature'
    },
    # 'hrdps-lam-nat-cont-etal': {'CAPE': 'convective-available-potential-energy',
    #     'DEPR': 'dew-point-depression',
    #     'HGT': 'geopotential-height',
    #     'RH': 'relative-humidity',
    #     'SPFH': 'specific-humidity',
    #     'HLCY': 'storm-relative-helicity',
    #     'TMP': 'temperature',
    #     'WDIR': 'wind-direction-from-which-blowing',
    #     'WIND': 'wind-speed',
    #     'UGRD': 'u-component-of-wind',
    #     'VGRD': 'v-component-of-wind'
    # }, 
    'hrdps-lam-nat-cont-htgl': {
        # 'DEN': 'density',
        # 'DEPR': 'dew-point-depression',
        'DPT': 'dew-point-temperature',
        # 'RH': 'relative-humidity',
        # 'SPFH': 'specific-humidity',
        'TMP': 'temperature',
        'WDIR': 'wind-direction-from-which-blowing',
        'WIND': 'wind-speed',
        'GUST': 'wind-speed-gust'
        # 'UGRD': 'u-component-of-wind',
        # 'VGRD': 'v-component-of-wind'
    }
}

dimensions = {
# 'DBLL': 'level,reference time,forecast hour',
'EATM': 'reference time,forecast hour',
# 'EtaL': 'level,reference time,forecast hour',
'HTGL': 'level,reference time,forecast hour',
'ISBL': 'level,reference time,forecast hour',
'MSL': 'reference time,forecast hour',
'NTAT': 'reference time,forecast hour',
'SFC': 'reference time,forecast hour'
}


levels_list = [100000, 90000, 80000, 70000, 60000, 50000, 40000, 30000]

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
if not data_dir.is_dir():
    raise Exception(f'The data directory {data_dir} does not exist.')


global_pairsHost = 'https://pairsupl-ib:5011'



# Product definition
spatial_coverage = {
    'lat' : (27, 70),    #(40, 85)
    'lon' : (-153, -43) #(-143, -51)
}


dataset_short_name = 'CMC-hrdps-continental-archive'

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

def process_band(band_number:int, grib_file:str, band:gdal.Band, dataset_info:dict, year, month, day, reproject:bool=False) -> gdal.Dataset:
        upload_dir = Path(f'/data/pairs-uploader/pre-uploader_datasets/hrdps.{year}{month:02}{day:02}/input/')
        if not upload_dir.is_dir():
            try:
                os.makedirs(upload_dir)
            except:
                logger.error(f'{upload_dir} already exists!')

        logger.info(f'upload_dir = {upload_dir}')
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

        layer_name = get_layer_name(grib_file, band_info['description'], band_info['metadata'], 'EPSG_4326', band_number)

        u_file = upload_dir / f'{layer_name}.tiff'
        if os.path.isfile(u_file):
            logger.warning(f'{u_file} exists!')
            return None, None, None


        logger.info(f'Processing {layer_name}')


        valid_time =  re.sub('([0-9]+) .*?$',r'\1',band_info['metadata']['GRIB_VALID_TIME'])
        ref_time = re.sub('([0-9]+) .*?$',r'\1',band_info['metadata']['GRIB_REF_TIME'])
        
        time_string = datetime.datetime.fromtimestamp(int(valid_time), tz=timezone.utc).timestamp()
        reference_time = datetime.datetime.fromtimestamp(int(ref_time), tz=timezone.utc).strftime('%Y%m%dT%H%M%SZ')
        
        m_file = u_file.with_suffix('.tiff.meta')
        url_str = 'http://pairsproxy.science.gc.ca/datasets/upload/' + str(os.path.basename(u_file))

        type_of_level = get_type_of_level(band_info['description'])

        if type_of_level not in datasets.keys():
            logger.warning(f'{type_of_level} not in {datasets.keys()}')
            return None, None, None

        dataset_id = datasets[type_of_level]

        if dataset_id not in layers.keys():
            logger.warning(f"{dataset_id} not in {layers.keys()}")
            return None, None, None

        if band_info['metadata']['GRIB_ELEMENT'] not in layers[dataset_id]:
            logger.warning(f"{band_info['metadata']['GRIB_ELEMENT']} not in {layers[dataset_id].keys()}")
            return None, None, None

        # datalayer_id = int(layers[dataset_id][band_info['metadata']['GRIB_ELEMENT']])    
        datalayer_key = layers[dataset_id][band_info['metadata']['GRIB_ELEMENT']]

        level = get_level(band_info['description'])

        if type_of_level == 'ISBL' and level not in levels_list:
            logger.warning(f"{level} not in {levels_list}")
            return None, None, None

        if type_of_level not in dimensions.keys():
            logger.warning(f"{type_of_level} not in {dimensions.keys()}")
            return None, None, None
            
        dims = dimensions[type_of_level]
        forecast_hour = get_forecast_hour(band_info['metadata'])


        if 'level' in dims:
            dimension_values = ','.join([str(level),str(reference_time),str(forecast_hour)])
        else:
            dimension_values =','.join([str(reference_time),str(forecast_hour)])


        logger.info(f'creating tiff and meta file {str(u_file)}')
        tmp_file = f'{uuid.uuid4()}.tiff'
        dst_ds = create_dataset_from_band(driver, tmp_file, dataset_info, band_info)

        if reproject:
            layer_name = get_layer_name(grib_file, band_info['description'], band_info['metadata'], 'EPSG_4326', band_number)
            u_file = upload_dir / f'{layer_name}.tiff'
            dst_ds = warp_to_wgs84(str(u_file), dst_ds)

        
        with m_file.open('w') as fp:
            fp.write(f"pairsdataset={dataset_id}\n")
            fp.write(f"pairsdatalayer={datalayer_key}\n")
            fp.write(f"timestamp={str(int(time_string))}\n")
            fp.write(f"pairsdatatype=2draster\n")
            fp.write(f"geospatialprojection=EPSG:4326\n")
            fp.write(f"pairsdimension={dims}\n")
            fp.write(f"dimension_value={dimension_values}\n")
            fp.write(f"band=1")

        if os.path.exists(tmp_file):
            os.remove(tmp_file)

        return dst_ds, u_file, m_file
            
        
def process_grib_file(grib_file:str, year, month, day) -> None:
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
        
        dst_ds, u_file, m_file = process_band(i, grib_file, band, dataset_info, year, month, day, reproject=True)

        # go to next band
        if dst_ds is None:
            continue

        shared_list.append((u_file,m_file))

        # cleanup 
        del dst_ds    

        # cleanup
        del band

    # cleanup
    del dataset


def upload_to_pairs(data_interval_start: pendulum.datetime):#, ti: TaskInstance) -> None:
    logger.info(f'{__file__} last modified on {datetime.datetime.fromtimestamp(os.path.getmtime(__file__)).replace(microsecond=0)}')
    logger.info(f'Current working directory is {os.getcwd()}')
    year = data_interval_start.year
    month = data_interval_start.month
    day = data_interval_start.day
    target_date = f'{year}{month:02}{day:02}'
    logger.info(f'Processing files from {target_date}')
    # target_date = '201701'

    targets_24h = ['00_000','00_024','00_048','06_018','06_42','12_012','12_036','18_006','18_030']

    filtered_grib_files = get_files_to_process(data_dir, target_date, targets_24h)

    logger.info(f'Processing {len(filtered_grib_files)} files')

    with multiprocessing.Pool(8) as p:
        p.starmap(process_grib_file, zip(filtered_grib_files, repeat(year), repeat(month),repeat(day)))


    upload_files = [x[0] for x in shared_list]
    meta_files = [x[1] for x in shared_list]

                
    logger.info(f'Generated {len(upload_files)} raster and {len(meta_files)} meta files.')

    with open(f'/data/pairs-uploader/pre-uploader_datasets/hrdps.{year}{month:02}{day:02}/tiff_gen_complete', 'w') as fp:
        fp.write('finished')
                

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
    schedule_interval='@daily',
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

