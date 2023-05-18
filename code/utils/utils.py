from google.cloud import storage
import pyspark.sql.functions as F

def convert_time(t, Hz=100):
    '''
    Convert integer steps to seconds based on a time-step freqnecy Hz. 
    '''
    return t / Hz


# NOTE: function taken and modified from GCP support documentation
def list_blobs(bucket_name, string_match=None):
    """Lists all the blobs in the bucket."""
    # bucket_name = "your-bucket-name"

    storage_client = storage.Client()

    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name)

    fl = []
    if string_match:
        for blob in blobs:
            if string_match in blob.name:
                file_name = blob.name.replace(string_match, '')
                fl.append(file_name)
    else:
        for blob in blobs:
            fl.append(blob.name)
        
    return fl


def feed_files(file_list, prefix=None, v=0, spark_session=None):
    '''
    Read in files from directory
    
    Inputs:
        file_list (list): strings of timeseries file names
        prefix (string): file path prefix to prepend to all file strings in file_list
        v (int): verbose level in {0: no verbosity, 1: verbosity}
        spark_session: spark instance created by SparkSession.builder.appName("PySpark Cloud Test").getOrCreate()
    
    Outputs:
        df (spark.sql.DataFrame): a dataframe of joined time series data
        
    '''
    c = 0
    for i, f in enumerate(file_list):

        if (v == 1) and (i % 25 == 0):
            print(f"File {i+1} of {len(file_list)}")


        try:
            id = f.replace(".csv", "")
            ts = spark_session.read.csv("gs://msca-bdp-student-gcs/"+prefix+f, header=True)
        except:
            print(f"WARNING - the following file could not be read: {f}")
            continue
        
        ts = ts.withColumn("Id", F.lit(id))

        if c == 0:
            df = ts
            c += 1
            continue
            
        df = df.union(ts)
        c += 1
    return df


def create_dummies(df, col_name):
    """
    In:
        df : spark dataframe
        col_name (str): column name for which you want to generate dummy variables
    Out: 
        df with dummy columns
    """
    import pyspark.sql.functions as F

    vals = df.select(col_name).distinct().collect()
    for v in vals:
        dummy = v.__getitem__(col_name)
        df = df.withColumn(dummy,
                      F.when((F.col(col_name) == dummy), 1) \
                      .otherwise(0))
    return df


def transform_target(df):
    '''
    Combine three separate target variables into one.
    '''
    df = df.withColumn('target', 
                      F.when((F.col("StartHesitation") == 1), 1) \
                           .when((F.col("Turn") == 1), 2)\
                           .when((F.col("Walking") == 1), 3) \
                           .otherwise(0) 
                      )
    return df