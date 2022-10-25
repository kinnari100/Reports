    
import pandas as pd
import io
import glob
import os
import datetime
import openpyxl
import xlsxwriter
from pandas import ExcelWriter
from pandas import Series,DataFrame
from openpyxl.drawing.image import Image
import shutil
import numpy as np
from pathlib import Path
from mi_utils import ensure_path


DATA_DIR_WORK = Path(Path.home(), '/Users/kinnaripatel/Desktop/k/MyProjects/Project3/Kin')

configuefile= ensure_path(DATA_DIR_WORK, 'KK.xlsx')

#print(file_name1)

#dfk = os.path.basename(file_name1)
#print(dfk)
#dfk1=dfk.rpartition('.xlsx')[0]
#print(dfk1)

configue=pd.read_excel(configuefile)
#print(configue)

  
for (ind) in configue.index:
    Nfilename=configue['Nfilename'][ind]
    Kfilename=configue['Kfilename'][ind]
    #print(Nfilename)
    #print(Kfilename)
    file_name = ensure_path(DATA_DIR_WORK, Kfilename)
    file_name1 = ensure_path(DATA_DIR_WORK, Nfilename)
    #print(file_name1.stem)
    facility_code = file_name1.stem.split('-')[1]
    #print(facility_code)
    data = pd.read_excel(file_name,index_col=None, usecols = "G", header = 31, nrows=4)


    data1 = pd.read_excel(file_name1,index_col=None, usecols = "G", header = 31, nrows=4)
    data.columns = pd.MultiIndex.from_tuples(zip(['KinnariECOE'], data.columns))    
    #print(data) 



    data1.columns = pd.MultiIndex.from_tuples(zip(['NataliaECOE'], data1.columns))    
    #print(data1)   

    result = pd.concat([data, data1], axis=1, join='inner')
    result
    #print(result)
    #print(list(result.columns.levels[1]))
  
    
    diff = result['KinnariECOE'] - result['NataliaECOE']
    diff.columns = pd.MultiIndex.from_tuples([('diff',col) for col in diff.columns])
    x=pd.concat([result,diff],axis=1)
    #print(x)
    x.columns = x.columns.droplevel(1)
   
    x = x.rename(index={0: facility_code,1:facility_code,2:facility_code,3:facility_code,4:facility_code})
    print(x)
    with pd.ExcelWriter('/Users/kinnaripatel/Desktop/k/MyProjects/Project3/NK.xlsx', engine='openpyxl',mode='a',if_sheet_exists='overlay') as writer:
            pd.DataFrame(x).to_excel(writer, sheet_name =facility_code)
    
    df = pd.concat(pd.read_excel('/Users/kinnaripatel/Desktop/k/MyProjects/Project3/NK.xlsx', sheet_name=None), ignore_index=True)
    df.to_excel('/Users/kinnaripatel/Desktop/k/MyProjects/Project3/kinju.xlsx',index=False)
