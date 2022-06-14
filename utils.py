import os,json,sys,csv,zipfile,re
import numpy as np
import pandas as pd
from pynamodb.models import Model as dynamoModel
from pynamodb.attributes import (
    UnicodeAttribute, NumberAttribute, UnicodeSetAttribute, BooleanAttribute,MapAttribute,JSONAttribute,ListAttribute
)
from dateutil.relativedelta import relativedelta as relativedelta
from dateutil.parser import parse
import datetime
import time,string,random
from typing import List, TypeVar,Callable,Union,Type,Tuple,Any,Dict
from typing import Callable
from typing import Tuple,Union
import pytz
from dateutil import tz


class MissingValueException(Exception):
    pass

def any_datetime_2_utc_timestamp(timeN:datetime.datetime)->float :
    if timeN.tzinfo is None:
        strptime=time.mktime(timeN.replace(microsecond=0).timetuple())
        return strptime
    else:
        timeN=timeN.astimezone(pytz.utc).replace(microsecond=0)
        return timeN.timestamp()


def utc_timestamp_2_utc_datetime(timeN:float)->datetime.datetime:
    return datetime.datetime.utcfromtimestamp( timeN).replace(tzinfo=pytz.utc)

def any_datetime_2_utc_isoformat(timeN:datetime.datetime)->str:
    return timeN.astimezone(pytz.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')

def any_isoformat_2_utc_datetime(timeN:str)->datetime.datetime:
    #return parse_datetime.astimezone(pytz.utc)
    return parse(timeN).astimezone(pytz.utc)

def china_isoformat_2_utc_datetime(timeN:str)->datetime.datetime:
    #return parse_datetime.astimezone(pytz.utc)
    return parse(timeN).replace(tzinfo=datetime.timezone(datetime.timedelta(hours=8))).astimezone(pytz.utc)

def utc_timestamp_2_utc_isoformat(timeN:float)->str:
    return any_datetime_2_utc_isoformat(utc_timestamp_2_utc_datetime(timeN))

def any_isoformat_2_utc_timestamp(timeN:str)->float:
    return any_datetime_2_utc_timestamp(any_isoformat_2_utc_datetime(timeN))

def datetime_utc_now()->datetime.datetime:
    return datetime.datetime.utcnow().replace(tzinfo=pytz.utc).replace(microsecond=0)
def timestamp_utc_now()->float:
    #return round(time.time() * 1000)/1000
    return any_datetime_2_utc_timestamp(datetime_utc_now())


class RefSurveiesStorage:
    AWS_DYNAMODB_TABLE_NAME = 'my-table'
    AWS_REGION_NAME = "ap-southeast-1"
    DROP_COLS=[]
    def __init__(self,key_path,ref_surveies_pool_path):
        with open(key_path, "r", encoding='UTF-8') as f:
            rows = list(csv.reader(f))[1][2:4]
            self.AWS_ACCESS_KEY_ID= rows[0]
            self.AWS_SECRET_ACCESS_KEY= rows[1]
            self.ref_surveies_pool_path=ref_surveies_pool_path
            
        class RawpushDynamoModel(dynamoModel):
            puuid = UnicodeAttribute(hash_key=True) #p-key  yyyy_mm-uuid
            timestamp = NumberAttribute(range_key=True) # sort key
            data=JSONAttribute()  # save as json  in Dynamodb,  and pull prase as json  {name:string, value:float }
            class Meta:
                table_name = self.AWS_DYNAMODB_TABLE_NAME
                region = self.AWS_REGION_NAME
                aws_access_key_id = self.AWS_ACCESS_KEY_ID
                aws_secret_access_key = self.AWS_SECRET_ACCESS_KEY
        self.STORE=RawpushDynamoModel
    def dyHash(self,date:datetime.datetime,uuidStr:str)->str:
        return "{2}_{0}-{1:0>2d}".format(date.year,date.month,uuidStr)
    def get_HEL(self,uuid,t_start:datetime.datetime,t_end:datetime.datetime):
        dyHash=self.dyHash(t_start,uuid,)
        t_float_earlier=any_datetime_2_utc_timestamp(t_start)
        t_float_later=any_datetime_2_utc_timestamp(t_end)
        dir_path=os.path.join(self.ref_surveies_pool_path,dyHash)
        csv_path=os.path.join(dir_path,f"{str(t_float_earlier)}.csv")
        return {
            'hash':dyHash,
            't_end': t_float_later,
            't_start':t_float_earlier,
            'dir_path':dir_path,
            'csv_path':csv_path
        }
    def downloadDataOneDay(self,hel)->pd.DataFrame:
        # print(self.dyHash(t_start,device_uuid,),)
        # print(any_datetime_2_utc_timestamp(t_start),any_datetime_2_utc_timestamp(t_end))
        # return
        m=utc_timestamp_2_utc_datetime(hel['t_start'])
        print(f'download {m} {hel["csv_path"]}')
        q= self.STORE.query(
                hel['hash'],
                range_key_condition=(
                    self.STORE.timestamp.between(hel['t_start'],hel['t_end'])
                    ),
                limit=5000
                )
        def to_json(d):
            d["data"]=json.dumps(d["setting"],ensure_ascii=False)
            return d
        df=pd.DataFrame([ to_json(item.attribute_values) for item in q ])
        if not os.path.exists(hel['dir_path']):
            os.makedirs(hel['dir_path'])
        df.to_csv(hel['csv_path'],index=False,encoding='utf-8')
        return df
        
    def getData(self,uuid,now:datetime.datetime,t_start:datetime.datetime,t_end:datetime.datetime)->pd.DataFrame:
        m_start_chunk=t_start.replace(hour=0,minute=0,second=0,microsecond=0)
        iday=-1
        latest_chunk=now.replace(hour=0,minute=0,second=0,microsecond=0)
        datas=[]
        while True:
            iday+=1
            this_chunk=m_start_chunk+relativedelta(days=iday)
            end_chunk=this_chunk+relativedelta(days=1)+relativedelta(seconds=-1)
            if this_chunk >=t_end:
                break
            hel=self.get_HEL(uuid,this_chunk,end_chunk)
            if self.checkIsStoredLocal(hel) and latest_chunk != this_chunk :
                datas.append(pd.read_csv(hel['csv_path']))
            else:
                datas.append(self.downloadDataOneDay(hel))
        df=pd.concat(datas,axis=0, ignore_index=True)
        df=self.df_process( df)
        mask = (df['timestamp'] > any_datetime_2_utc_timestamp(t_start) ) & (df['timestamp'] < any_datetime_2_utc_timestamp(t_end) )
        df=df[mask]
        df.reset_index(drop=True, inplace=True)
        return df
    def df_process(self, df):
        #print(df)
        def serializer(x):
            l=json.loads(x)
            d={}
            for item in l:
                d[item["name"]]=item["value"]
            return d
        df2=pd.DataFrame(df["data"].map(serializer).tolist())
        df.drop(self.DROP_COLS+["data"], axis=1, inplace=True)
        return pd.concat([df,df2], join='outer',axis=1)
    def checkIsStoredLocal(self,hel):
        return os.path.exists(hel['csv_path'])



        

class RefSurveiesVender:
    def __init__(self,surv_storage:RefSurveiesStorage,uuid,now,t_start,t_end):
        self.datas=surv_storage.getData(uuid,now,t_start,t_end)
        self.now=now
        self.device_uuid=uuid
        self.t_start=t_start
        self.t_end=t_end
        
    def getData(self):
        return self.datas

    def getDataByDateList(self,regression_date_list)->List [pd.DataFrame]:
        """shift back 1~2 hour to o'clock  with whole 1 hour data
        """

        r=[]
        for date in regression_date_list:
            new_date_end=date.replace(minute=0,second=0,microsecond=0)+relativedelta(hours=-1)
            new_date_start=date.replace(minute=0,second=0,microsecond=0)+relativedelta(hours=-2)
            if self.t_start > new_date_start:
                raise Exception(f"request the earliest date is {new_date_start}")
            mask = (self.datas['timestamp'] >= any_datetime_2_utc_timestamp(new_date_start) ) & (self.datas['m_time'] <= any_datetime_2_utc_timestamp(new_date_end) )
            r.append(self.datas[ mask  ])
        return r
    def get_value_array_of_na(self,sensor_order,df:pd.DataFrame)->np.ndarray:
        def get(x):
            return x.values[0]
        if isinstance(df,pd.Series):
            def get(x):
                return x
        def fn(name):
            unittype=get_sesnor_unit(name)
            input=get(df[name])
            if pd.isna(input) :
                raise MissingValueException
            return unit_converter_na(unittype,input) 
        return np.asarray([ fn(i)  for i in sensor_order ])
    
    def check_diff_zreo(self,point_surv_i:np.ndarray,point_surv_j:np.ndarray)->bool:
        return np.any(np.isin(point_surv_i-point_surv_j,[0]))




def getIndex_of_current_date_and_before(current_date:Union[datetime.datetime,str],date_list):
    date=None
    if isinstance(current_date,datetime.datetime):
        date=current_date
    elif isinstance(current_date,str):
        date=any_isoformat_2_utc_datetime(current_date) 
    else:
        raise Exception(f"wrong current_date type: {type(current_date)}")

    l=[ i for i,item in enumerate( date_list) if item <= current_date]
    return l
            
        

