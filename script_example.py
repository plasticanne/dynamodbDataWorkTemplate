import os
import utils
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta as relativedelta


def data_work(surv_df:pd.DataFrame):
    
    import matplotlib.pyplot as plt  
    surv_df.plot(x='timestamp',y=["value"])
    plt.show()




if __name__ == '__main__':
    key_path = os.path.join('../../dydb-key.csv')
    #本地資料庫存
    ref_surveies_pool_path='D:\\dataStroage'
    uuid='xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxx' 
    #設定掃描時間區間與現在時間
    now=utils.datetime_utc_now()
    t_start=utils.any_isoformat_2_utc_datetime("2022-04-18 00:10:00Z")  #Z is UTC
    t_end=utils.any_isoformat_2_utc_datetime("2022-04-18 23:59:59Z") #Z is UTC
   

    #實例化資料昌庫
    surv_storage=utils.RefSurveiesStorage(key_path,ref_surveies_pool_path)
    #實例化資料提取工具
    surv_vender=utils.RefSurveiesVender(surv_storage,uuid,now,t_start,t_end)
    

    #自訂資料處理
    data_work(surv_vender.getData())



    





