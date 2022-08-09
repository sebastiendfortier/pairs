# ERA5 folder

## Structure

- One folder per dataset
- In each folder
  * Dags folder:  contains the actual scripts to do the ingest using airflow
  * Datasets: contains the dataset definitions
  * Layers: contains the layer definitions

# HRDPS folder

## Structure

- Dags folder:  contains the actual scripts to do the ingest using airflow
  * the latest files in use are hrdps_datamart_v4.py (live datamart ingestion) and hrdps_lam_grib2_nat_cont_DAG_v3.py (archive ingestion)
- Datasets: contains the dataset definitions
  * for the hrdps archives there are multiple datasets present in each processed file
- Layers: contains the layer definitions
  * In this folder you will find one file that defines each layer of a specific dataset
- Dimensions: contains the dimension definitions
  * In this folder you will find one file that defines each dimension for of a specific dataset

# notebooks folder

- Here you will find ndifferent notebooks that I have used to analyse the datasets / files
  * grib2 analysis using gdal python lib
  * fst file analysis using science fstpy python lib
  * emet observations database analysis using postgres connector in python
  * era5 netcdf file analysis
  * CMC archive system analysis
  * and others (needs cleaning and organizing)
 
