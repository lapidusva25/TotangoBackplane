from fetchTouchpointsV1 import gettouchpoints
import pandas as pd
from s3ops import FileOutput, GetS3File

key = 'CSVNameAndPath'
df_jobs = pd.read_csv(GetS3File(key), dtype=str, error_bad_lines = False, encoding='latin1')

for job_index,job in df_jobs.iterrows():
	
	key=job['folderName']+'/'+job['sourceDatacenter']+'-'+job['sourceInstance']+'-to-'+job['destinationInstance']+'-'+job['Level']+'-touchpoints.csv'
	print(key)
	try:
		job_df = gettouchpoints(job['token'],job['segmentID'],job['Relationship']).copy(deep=True)
		valid=True
	except AttributeError:
		True
		valid=False

	if valid:
		FileOutput(job_df,key)
