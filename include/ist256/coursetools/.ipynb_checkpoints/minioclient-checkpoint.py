from minio import Minio
from minio.error import ResponseError

class MinioClient(object):
    
    def __init__(self):
        minio_credentials = {
            'host' : '10.30.24.123:9000', 
            'key' : 'sesuro5pka32vtt',
            'secret' : 'c5977GQW2CHF6wsNG5bK', 
            }
        self.__mc = Minio(minio_credentials['host'], access_key=minio_credentials['key'], secret_key=minio_credentials['secret'], secure=False)
        
    def bucket_exists(self, bucket_name):
        return self.__mc.bucket_exists(bucket_name)
    
    def make_bucket(self,bucket_name):
        if not self.bucket_exists(bucket_name):
            self.__mc.make_bucket(bucket_name)
        return self.bucket_exists(bucket_name)
    
    def exists(self, bucket_name, remote_filespec):
        items = self.__mc.list_objects(bucket_name,prefix=remote_filespec)
        for item in items:
            if item.object_name==remote_filespec:
                return True
        return False    
    def put(self, bucket_name, local_filespec, remote_filespec):
        with open (local_filespec, 'rb') as f:
            stats = os.stat(local_filespec)
            etag = self.__mc.put_object(bucket_name, remote_filespec, f , stats.st_size )
            return etag
    
    def get(self, bucket_name, remote_filespec):
        return self.__mc.get_object(bucket_name, remote_filespec)        
        
    def fput(self,bucket_name,local_filespec, remote_filespec):
        etag = self.__mc.fput_object(bucket_name, remote_filespec, local_filespec)
        return etag
        
    def fget(self,bucket_name,remote_filespec, local_filespec):
        info = self.__mc.fget_object(bucket_name, remote_filespec, local_filespec)
        return info
        