## Anaylsis of Integrated Surface Data "Lite" from NOAA weather stations

- The Integrated Surface Database (ISD) consists of global hourly and synoptic observations compiled from numerous sources.
- The database includes over 35,000 stations worldwide, with some having data as far back as 1901, though the data show a substantial increase in volume in the 1940s and again in the early 1970s. Currently, there are over 14,000 "active" stations updated daily in the database.
- **The Goal of the challenge :** is to draw some useful insights from the ISD data.
 The **following documents** give some understaning of the available data.
     https://www1.ncdc.noaa.gov/pub/data/noaa/isd-lite/isd-lite-format.txt
     https://www1.ncdc.noaa.gov/pub/data/noaa/isd-lite/isd-lite-technical-document.txt

**The actual weather station data files can be found at:**
     https://www1.ncdc.noaa.gov/pub/data/noaa/isd-lite/
The data filenames correspond with the station numbers listed in the isd-history.txt file listed below.  For example, 716230-99999-2010.gz corresponds with USAF number 716230 (London Station) and WBAN number 99999.
     ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.txt

### Solution Design

#### Modularized the solution as below :
##### 1. A Python Program : 
 The program downloads the files from the site :  https://www1.ncdc.noaa.gov/pub/data/noaa/isd-lite/  and stores into local file system. Each of them is a gzip file, representing a stations data in a particular year. 
 The program downloads files for a specific range of years mentioned in the code. For simplicity sake, it is set to choose 5 years worth of data from 2013 - 2018. All the files are downloaded to one folder in local file system.
##### 2. Spark/Scala Program :
The program does the following:
- Reads the files from local file system directory.
- Applies a schema based on the format of the file. The file format is defined as fixed length and the description can be found at the following link:
https://www1.ncdc.noaa.gov/pub/data/noaa/isd-lite/isd-lite-format.txt
- Creates a Spark session and SQL Context. Creates a DataFrame reading the files from the local file sytem folder.
- Extracts the file name as a new column to each row.
- Partitions the data per year and saves it as a parquet file into the local system folder.
##### 3. PySpark program :
The program uses Pandas/Matplotlib/Seaborn for insights into data stored as parquet files.
It uses PySpark to read data from the parquet file and create DataFrames for analysis. Creates a SparkSession, SQLContext for reading Parquet file and extracts as a DataFrame for usage.
The DataFrame extracted is converted to Pandas DataFrame for plotting charts/graphs. Further prepping of DataFrame is done for usage with various data insights.
