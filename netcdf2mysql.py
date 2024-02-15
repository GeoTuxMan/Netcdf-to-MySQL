import os
import shutil
import time
import glob
import sys
import zipfile
import smtplib
import string
import xarray as xr
from netCDF4 import Dataset
from scipy.io import netcdf
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import sqlalchemy


os.chdir("C:\\Users\\Vostro\\Desktop\\Syroco")
root = os.getcwd()
ds = xr.open_dataset(root + "\\" +"BS_1d_20231201_20231201_grid_T_20231201-20231201.nc4")
print(ds.data_vars) # existing variables in netcdf file
# get salinity
sal= ds['vosaline'].sel(deptht=2.5045619010925293,time_counter='2023-12-01T12:00:00', method='nearest')
variable = 'vosaline'
print('The unit of the variable', variable, 'is', ds[variable].attrs['units'])

# convert to dataframe
df = sal.to_dataframe()
# extract the required fields
df_subset = df[['nav_lat', 'nav_lon', 'deptht', 'time_centered', 'vosaline']]
# remove nan values
df2=df_subset.dropna()
#extract only first 100 records
mini = df2.head(100) 
  
# insert in mysql database
database_username = 'root'
database_password = ''
database_ip       = 'localhost'
database_name     = 'syroco'
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))
mini.to_sql(con=database_connection, name='salinity2', if_exists='append')



# close connection
database_connection.dispose
