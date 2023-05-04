from google.cloud import storage
from pyspark.sql.functions import lit

def convert_time(t, Hz=100):
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


def feed_files(file_list, prefix=None, v=0, spark=None):
    '''
    Read in files from directory
    
    Inputs:
        file_list (list): strings of timeseries file names
        prefix (string): file path prefix to prepend to all file strings in file_list
        v (int): verbose level in {0: no verbosity, 1: verbosity}
    
    Outputs:
        df (spark.sql.DataFrame): a dataframe of joined time series data
        
    '''
    c = 0
    for i, f in enumerate(file_list):

        if (v == 1) and (i % 10 == 0):
            print(f"File {i+1} of {len(file_list)}")
        id = f.replace(".csv", "")

        try:
            ts = spark.read.csv("gs://msca-bdp-student-gcs/"+prefix+f, header=True)
        except:
            print(f"WARNING - the following file could not be read: {f}")
            continue
        
        ts = ts.withColumn("Id", lit(id))

        if c == 0:
            df = ts
        else:
            df.union(ts)

        df.union(ts)
        c += 1
    return df