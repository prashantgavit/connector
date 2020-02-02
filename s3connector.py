import boto3
from io import BytesIO, StringIO
import pandas as pd

class S3connection:
    '''
    This class is used to create and s3 connector instance which can be further used to
        1. Create bucket
        2. Upload/fetch data
      
     Arg:
         access_key(str) : aws acccount access key
         access_secret_key(str) : aws account secret access key
         region : aws machine region where data will be processed
    '''
    def __init__(self,access_key = None, access_secret_key = None,region = None):            
            self.access_key = access_key
            self.access_secret_key = access_secret_key
            self.region = region
            self.s3_session = boto3.Session(aws_access_key_id=self.access_key, aws_secret_access_key=self.access_secret_key)
        
    def create_bucket(self,bucket_name):
        if self.check_if_bucket_exists(bucket_name) == True:
            print('Bucket already exists')
        else:
            if self.region is None:
                s3_client = self.s3_session.client('s3')
                print(s3_client.create_bucket(Bucket=bucket_name))
            else:
                s3_client = self.s3_session.client('s3', region_name=self.region)
                location = {'LocationConstraint': region}
                print(s3_client.create_bucket(Bucket=bucket_name,CreateBucketConfiguration=location))
  
    def check_if_bucket_exists(self,bucket_name):
        return(bucket_name in [i['Name'] for i in self.s3_session.client('s3').list_buckets()['Buckets']])
    

            
    def upload_to_s3(self,bucket,location,fileName,data):
        '''
        Args:
            bucket (str): bucket name in which data will be stored.
            location (str): location on bucket where data will be stored.
            fileName (str): Name that should be given to stored file.
            data(any type of data): however this function was created only for pandas dataframe and some standardized python dataframes 

        Returns:
            message(str): if data has been stored on S3 or not
        '''
        
        data_buffer = StringIO()
        s3_resource = self.s3_session.resource('s3')
        if self.check_if_bucket_exists(bucket) == False:
            print('Bucket does not exist')
        else:
            if isinstance(data,pd.DataFrame):
                try:
                    csv_buffer = StringIO()   
                    data.to_csv(csv_buffer, index = False)
                    s3_resource.Object(bucket, location + fileName).put(Body=csv_buffer.getvalue())
                    print('data had been uploaded on s3 using StringIO')

                except:
                    try:
                        csv_buffer = BytesIO()   
                        data.to_csv(csv_buffer, index = False) 
                        s3_resource.Object(bucket, location + fileName).put(Body=csv_buffer.getvalue())
                        print('data has been uploaded on s3 using BytesIO')
                    except:
                        print('pandas data frame is not getting uplpoaded in S3')

            else:
                try:
                    data_buffer.write(data)
                    s3_resource.Object(bucket, location + fileName).put(Body=data_buffer.getvalue())
                except:
                    print('data is not getting uploaded in S3')
                    
                    
    def get_from_s3(self,bucket,file_location):
        '''
        This function used to read txt file stored in s3
        Args:
            bucket(str): bucket from which data will be fetched
            file_loction(str) : file location
        Returns:
            body(str): string output of text file
        '''
        if self.check_if_bucket_exists(bucket) == False:
            print('Bucket does not exist')
        else:
            try:
                s3_resource = self.s3_session.resource('s3')
                obj = s3_resource.Object(bucket,file_location)
                body = obj.get()['Body'].read()
                return(body)
            except:
                print('Please check the file path')
                
    def read_csv_in_pandas_dataframe(self,bucket,file_location):
        '''
        This function used to read csv file stored in s3 in pandas dataframe format
        Args:
            bucket(str): bucket from which data will be fetched
            file_loction(str) : file location
        Returns:
            pandas dataframe output of csv file
        '''
        if self.check_if_bucket_exists(bucket) == False:
            print('Bucket does not exist')
        else:
            s3_client = self.s3_session.client('s3')
            obj = s3_client.get_object(Bucket = bucket, Key = file_location)
            return(pd.read_csv(BytesIO(obj['Body'].read()),dtype = str))

            
    def get_filename_list(self,bucket,location):
        '''
        This function returns the list of filename available in given bucket and filepath'
        Args:
            bucket(str): bucket from which data will be fetched
            loction(str) : location from where list of filename is needed to be extracted
        Returns:
            python list with filename
        '''
        return [i['Key'].replace(location,'') for i in \
                self.s3_session.client('s3').list_objects(Bucket = bucket \
                                                                  ,Prefix = location,Delimiter='/')['Contents']]

    
    
    

