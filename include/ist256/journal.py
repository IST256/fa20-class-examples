import os 
import socket 
import json
import requests
import ipykernel
import time
from datetime import datetime, timezone
from dateutil import parser, tz
import pandas as pd
import urllib3
from minio import Minio
from minio.error import ResponseError
import logging



class Journal:
    
    def __init__(self, minio_server='10.30.24.123:9000', access_key='sesuro5pka32vtt',secret_key='c5977GQW2CHF6wsNG5bK', debug=False):

        if debug:
            logging.basicConfig(format='%(asctime)s %(levelname)s ==> %(message)s', level=logging.DEBUG,datefmt='%m/%d/%Y %I:%M:%S %p')
        else:
            logging.basicConfig(format='%(asctime)s %(levelname)s ==> %(message)s', level=logging.INFO,datefmt='%m/%d/%Y %I:%M:%S %p')
        
        self.__timezone__ = "America/New_York"
        self.set_timezone()

        logging.debug(f"Minio Info {minio_server}")
        self.__mc__ = Minio(minio_server, access_key=access_key, secret_key=secret_key, secure=False)

        self.__netid__ = self.get_netid()
        self.__notebook__ = self.get_notebook_path()
        self.__notebook_full_path__ = f"{os.environ.get('HOME')}/{self.__notebook__}"
        self.__course__, self.__term__, self.__unit__, self.__assignment__, self.__assignment_type__ = self.parse_notebook_path()
        self.__bucket__ = f"{self.__course__}-{self.__term__}"
  
        self.initialize_bucket()

    def set_timezone(self):
        '''
        save on a lot of date math.
        '''
        os.environ['TZ'] = self.__timezone__
        time.tzset()
        
    
    def initialize_bucket(self):        
        if not self.__mc__.bucket_exists(self.__bucket__):
            self.__mc__.make_bucket(self.__bucket__)
                
    def format_date(self,date):
        return date.strftime("%Y-%m-%d %I:%M:%S %p")
        
    def debug(self):        
        logging.debug(f"NETID       = {self.__netid__}")
        logging.debug(f"PATH        = {self.__notebook__}")
        logging.debug(f"FULL PATH   = {self.__notebook_full_path__}")
        logging.debug(f"COURSE      = {self.__course__}")
        logging.debug(f"TERM        = {self.__term__}")
        logging.debug(f"BUCKET      = {self.__bucket__}")
        logging.debug(f"JOURNAL     = {self.get_journal_path(self.__netid__)}")        
        logging.debug(f"TIME ZONE   = {self.__timezone__}")
        return            
    
    
    # TODO: Remove netid from the arguments
                
    def get_journal_path(self, netid = None):
        if netid == None:
            netid = self.__netid__
            
        return f"journal/{netid}.csv"
    
 
    def journal_exists(self, netid = None):
        journal_path = self.get_journal_path(netid)
        
        logging.debug(f"CMD: journal_exists netid={netid}, journal_path={journal_path}")
        

    def init_journal(self, netid = None):
        tmp = "_tmp.csv"
        journal_path = self.get_journal_path(netid)
        
        logging.debug(f"CMD: init_journal netid={netid}, journal_path={journal_path}")
        dataframe = pd.DataFrame( { 'Date' : [], 'Hours' : [], 'Comments' : [] } )
        dataframe.to_csv(tmp,index=False)
        etag = self.__mc__.fput_object(self.__bucket__,journal_path,tmp)
        os.remove(tmp)
        logging.debug("DONE: init_journal")
        return dataframe
        
    
    def load_journal(self, netid = None):
        
        journal_path = self.get_journal_path(netid)
        
        logging.debug(f"CMD: load_journal netid={netid}, journal_path={journal_path}")
        
        try:
            dataframe = pd.read_csv(self.__mc__.get_object(self.__bucket__, journal_path))
        except: # ugh....
            dataframe = self.init_journal(netid)
           
        return dataframe
    
    def save_journal(self, dataframe, netid = None):
        tmp = "_tmp.csv"
        journal_path = self.get_journal_path(netid)
        
        logging.debug(f"CMD: save_journal netid={netid}, journal_path={journal_path}")
        dataframe.to_csv(tmp,index=False)
        etag = self.__mc__.fput_object(self.__bucket__,journal_path,tmp)
        os.remove(tmp)

        return etag 
        
    def get_netid(self):
        netid = os.environ.get("JUPYTERHUB_USER").lower()
        hostname = socket.gethostname().lower()
        callback_url = os.environ.get("JUPYTERHUB_OAUTH_CALLBACK_URL").lower()
        activity_url = os.environ.get("JUPYTERHUB_ACTIVITY_URL").lower()
        if callback_url.find(netid)>=0 and activity_url.find(netid)>=0 and hostname.find(netid)>=0:
                return netid
        else:
            raise ValueError(f"Unable to locate NetID={netid} for hostname {hostname}")

    def get_notebook_path(self):
        connection_file = os.path.basename(ipykernel.get_connection_file())
        kernel_id = connection_file.split('-', 1)[1].split('.')[0]
        token = os.environ.get("JUPYTERHUB_API_TOKEN")
        netid = self.__netid__
        response = requests.get(f'http://127.0.0.1:8888/user/{netid}/api/sessions?token={token}')
        response.raise_for_status()
        sessions = response.json()    
        for sess in sessions:
            if sess['kernel']['id'] == kernel_id:
                return sess['notebook']['path']
                break

    def parse_notebook_path(self):
        items = self.__notebook__.split("/")
        if items[5].startswith("CCL"):
            assign_type="Lab"
        elif items[5].startswith("HW") or items[5].startswith("NYC"):
            assign_type="Homework"
        else:
            assign_type = "Unknown"
        return items[1], items[2],items[4], items[5], assign_type
    
    
    