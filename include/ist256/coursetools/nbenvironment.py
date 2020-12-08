import os 
import ipykernel
import requests 
import json
import datetime
from datetime import datetime, timezone
import time
from .minioclient import MinioClient
from .settings import Settings

class NbEnvironment(object):
    
    def __init__(self):

        # compositions
        self.__minio_client = MinioClient()
        self.__settings = Settings().load()
        
        # properties
        self.__netid = self.__find_netid()
        self.__notebook_path = self.__find_notebook_path()
        self.__service_prefix = self.__find_service_prefix()
        self.__course = self.__find_course()
        self.__git_folder = self.__find_git_folder()
        self.__bucket = self.__find_bucket()
        self.__filename = self.__find_filename()
        self.__lesson = self.__find_lesson()
        self.__filespec = self.__find_filespec()
        self.__run_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        
        # timezone
        self.__set_timezone("America/New_York")

    def __set_timezone(self, tz_string):
        self.__timezone = tz_string
        os.environ['TZ'] = self.__timezone
        time.tzset()
        
    
    @property
    def properties(self):
        '''
        Return all properties as a dictrionary
        '''
        tmp = {}
        for key in self.__dict__.keys():
            tmp[key.replace('_NbEnvironment__','')] = self.__dict__[key]
        return tmp

    @property
    def timezone(self):
        return self.__timezone
    
    @property 
    def settings(self):
        return self.__settings
    
    @property 
    def netid(self):
        return self.__netid

    @property 
    def notebook_path(self):
        return self.__notebook_path

    @property 
    def service_prefix(self):
        return self.__service_prefix
    
    @property 
    def course(self):
        return self.__course
    
    @property 
    def git_folder(self):
        return self.__git_folder
    
    @property 
    def bucket(self):
        return self.__bucket
    
    @property
    def filename(self):
        return self.__filename
    
    @property
    def lesson(self):
        return self.__lesson
    
    @property
    def filespec(self):
        return self.__filespec
    
    @property 
    def run_datetime(self):
        return self.__run_datetime
    
    

    def __find_filespec(self):
        return f"{os.environ.get('HOME')}/{self.__notebook_path}"
        
    def __find_bucket(self):
        return f"{self.__course}-{self.__git_folder}"
            
    def __find_git_folder(self):
        items = self.__notebook_path.split('/')
        if len(items) >= 3 and items[0] == 'library':
            return self.__settings.get('git-folder',items[2])
        else:
            raise Error('This notebook file is not in a git folder under the course folder.')

    def __find_lesson(self):
        items = self.__notebook_path.split('/')
        if len(items) >= 4 and items[0] == 'library':
            return items[-2]
                
    def __find_filename(self):
        items = self.__notebook_path.split('/')
        if len(items) >= 4 and items[0] == 'library':
            return items[-1]
    
    def __find_course(self):
        items = self.__notebook_path.split('/')
        if len(items) >= 2 and items[0] == 'library':
            return items[1]
        else:
            raise Error('This notebook file must be in a course folder.')
            
    def __find_service_prefix(self):
        return os.environ.get('JUPYTERHUB_SERVICE_PREFIX')

    def __find_netid(self):
        netid = os.environ.get('JUPYTERHUB_USER')
        if os.environ.get('JUPYTERHUB_CLIENT_ID').find(netid)>=0 and os.environ.get('JUPYTERHUB_SERVICE_PREFIX').find(netid)>=0:
            return netid
        else:
            raise Error('Unable to locate a netid for this user.')
            
    def __find_notebook_path(self):
        connection_file = os.path.basename(ipykernel.get_connection_file())
        kernel_id = connection_file.split('-', 1)[1].split('.')[0]
        token = os.environ.get("JUPYTERHUB_API_TOKEN")
        netid = self.__netid
        response = requests.get(f'http://127.0.0.1:8888/user/{netid}/api/sessions?token={token}')
        response.raise_for_status()
        sessions = response.json()    
        for sess in sessions:
            if sess['kernel']['id'] == kernel_id:
                return sess['notebook']['path']

            
        