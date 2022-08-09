from osgeo import gdal, osr
from pathlib import Path
import copy
import datetime
import glob
import logging
import numpy as np
import re
import pandas as pd
import multiprocessing
base_path = Path('/fs/site6/eccc/cmd/w/sbf000/operation.forecasts.lamgrib2.hrdps_national.continental/')
target_date = '20200101'
grib_files = glob.glob(str(base_path / f'{target_date}*.grib2'))
# records = []
# names = []
# descriptions = []
# units = []
# type_of_level_names = []
# type_of_level_descriptions = []
# levels = []
# level_units = []

def process_band(dataset,i):
    # get current band
    band = dataset.GetRasterBand(i)
    description = band.GetDescription()
    # print(description)
    level = copy.copy(description)
    # print(level)
    type_of_level = copy.copy(description)
    level_unit = copy.copy(description)
    level = re.sub('(.*?)\[.*? .*?=.*$',r'\1',level)
    if '-' in level:
        level = '@'.join(level.split('-'))

    # level  = 
    # print(level)
    type_of_level_name = re.sub('.*? (.*?)=.*$',r'\1',type_of_level)
    # print(type_of_level_name)
    type_of_level_description = re.sub('.*? .*?=(.*$)',r'\1',type_of_level)
    # print(type_of_level_description)
    level_unit = re.sub('(.*?) .*?=.*$',r'\1',level_unit)
    level_unit = level_unit.replace(level,'')
    level_unit = re.sub('.*\[(.*?).$',r'\1',level_unit)
    level_unit = level_unit.replace('-','')
    type_of_level_description = type_of_level_description.replace('"','')

    meta = band.GetMetadata()
    mydict = {}
    mydict['name'] = meta['GRIB_ELEMENT']
    mydict['description'] = meta['GRIB_COMMENT'].replace(meta['GRIB_UNIT'],'')
    mydict['unit'] = meta['GRIB_UNIT'].replace('[','').replace(']','')
    mydict['type_of_level'] = type_of_level_name
    mydict['type_of_level_description'] = type_of_level_description
    mydict['level'] = level
    mydict['level_units'] = level_unit

    # names.append(meta['GRIB_ELEMENT'])
    # descriptions.append(meta['GRIB_COMMENT'])
    # units.append(meta['GRIB_UNIT'])
    # type_of_level_names.append(type_of_level_name)
    # type_of_level_descriptions.append(type_of_level_description)
    # levels.append(level)
    shared_list.append(mydict)



# for f in grib_files:
#         dataset = gdal.Open(f)
#         for i in range(1,dataset.RasterCount+1):
#             process_band(dataset,i)

def process_file(f):
    print(f'Processing {f}')
    dataset = gdal.Open(f)
    for i in range(1,dataset.RasterCount+1):
        process_band(dataset,i)
    

print(f'Processing {len(grib_files)} files\n')
# # for f in grib_files:
# #     process_file(f)



manager = multiprocessing.Manager()
shared_list = manager.list()
with multiprocessing.Pool(multiprocessing.cpu_count()) as p:
        p.map(process_file, grib_files)

print(f'Finished processing {len(grib_files)} files\n')


# df = pd.DataFrame({'name':names,'description':descriptions,'unit':units, 'type_of_level':type_of_level_names, 'type_of_level_description':type_of_level_descriptions, 'level': levels, 'level_units':level_units})
df = pd.DataFrame(list(shared_list))

df = df.drop_duplicates()#.sort_values('name')


groups = df.groupby(['name', 'unit', 'type_of_level', 'level_units'])
df_list = []
for _,cur_df in groups:
    # display(cur_df)
    if len(cur_df.index) > 1:
        levels = ','.join(cur_df.level.tolist())
        types = ','.join(cur_df.type_of_level.tolist())
        new_df = pd.DataFrame([cur_df.iloc[0].to_dict()])
        new_df['level'] = levels
        cur_df = copy.deepcopy(new_df)
    df_list.append(cur_df)


df = pd.concat(df_list,ignore_index=True).sort_values('name')
# df['id'] = np.arange(len(df.index)) + 1

# df
import os
csv_file = 'data_layers.csv' 
if os.path.isfile(csv_file):
    os.remove(csv_file)

df.to_csv(csv_file,index=False)
ndf = pd.read_csv(csv_file)
pd.options.display.max_rows = 999
print(ndf)
