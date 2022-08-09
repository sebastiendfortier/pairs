#!/bin/env python3

import glob
from pathlib import Path
import numpy as np
import shutil
import sys
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


# display(files)

def create_dir(dir):
    logger.info(f'creaing directory {dir}')
    Path(dir).mkdir(parents=True, exist_ok=True)

def copy_file(orig,dest):
    logger.info(f'copy {orig} -> {dest}')
    shutil.copyfile(orig, dest)

# display(os.path.abspath(files[0]))

def process_files(files):
    for f in files:
        fname = os.path.basename(f)
        dname = os.path.dirname(f)
        year = fname[0:4]
        month = fname[4:6]
        day = fname[6:8]
        hh = fname[8:10]
        p = fname[11:14]
        dir = f'{dname}/{year}/{month}/{day}/{hh}/{p}'
        # create_dir(dir)
        logger.info(f'{f} -> {dir}/{fname}')

        shutil.copyfile(f, f'{dir}/{fname}')
        # TODO
        # check if the copy is ok
        # remove copied file



def main():

    if len(sys.argv) == 1:
        logger.error('Please provile a path to process!')
        return 1

    path = os.path.abspath(sys.argv[1])
    logger.info(f'Processing {path} directory')
    files = np.sort(glob.glob(str( f'{path}/*.grib2')))
    process_files(files)

    return 0

if __name__ == '__main__':
    sys.exit(main())

