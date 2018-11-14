import dask
import dask.dataframe as dd
import dask.bag as db
import pandas as pd
import json
import logging

from dask.diagnostics import ProgressBar 
ProgressBar().register()

logger = logging.getLogger(__name__)

#======================================
#======================================
def convertToDict1(x):
    """
    to deal with funny column where item is string representing 
    a python obj
    the obj is a len 0 or 1 list where the 1st elem is a dict
    """ 
    x = eval(x) 
    if len(x)>1: raise IndexError("len>1 ", x) 
    if len(x)==0: return {} 
    return x[0] 
#======================================
#generally useful funcs
#======================================
def dropConstantCols(df):
    desc = df.describe(include='all')
    totCount = len(df)
    for colKey,colDesc in desc.iteritems():
        if (colDesc.get('count')==0):
            logger.info('All N/A in {}'.format(colKey))
            del df[colKey]
            continue

        if ((colDesc.get('count')==totCount) and
            ((colDesc.get('unique')==1) or (colDesc.get('std')==0.))):
            logger.info('No variation in {}'.format(colKey))
            del df[colKey]

def explodeDictCol(df, colname):
    fields = {
        'customDimensions' : [
            'index',
            'value',
        ],
        'device': [
            'browser',
            'browserSize',
            'browserVersion',
            'deviceCategory',
            'flashVersion',
            'isMobile',
            'language',
            'mobileDeviceBranding',
            'mobileDeviceInfo',
            'mobileDeviceMarketingName',
            'mobileDeviceModel',
            'mobileInputSelector',
            'operatingSystem',
            'operatingSystemVersion',
            'screenColors',
            'screenResolution'
        ],
        'geoNetwork': [
            'city',
            'cityId',
            'continent',
            'country',
            'latitude',
            'longitude',
            'metro',
            'networkDomain',
            'networkLocation',
            'region',
            'subContinent'
        ],
        'totals': [
            'bounces',
            'hits',
            'newVisits',
            'pageviews',
            'transactionRevenue',
            'visits'
        ],
        'trafficSource': [
            'adContent',
            'adwordsClickInfo',
            'campaign',
            'campaignCode',
            'isTrueDirect',
            'keyword',
            'medium',
            'referralPath',
            'source'
        ],
    }
    #this explode a column of dicts to columns of its keys
    #dfOut = df[colname].apply(convertList,dtype='object')
    tmp = [(c,'object') for c in fields.get(colname)]
    dfOut = df[colname].to_bag().to_dataframe(meta=tmp) 
    dfOut.columns = ["%s.%s"%(colname,subcolname) for subcolname in dfOut.columns]
    #dfOut = dd.concat([df, dfOut], axis='columns')
    #del dfOut[colname]
    return dfOut


def configLogger(level = logging.ERROR):
    logger = logging.getLogger(__name__)
    logger.setLevel(level)
    fh = logging.FileHandler('logging.log')
    fh.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('[%(levelname)s]%(asctime)s:%(name)s:%(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)

if __name__=="__main__":
    configLogger(logging.INFO)

    # fileList = ["shard.csv",]
    fileList = ["train_v2.csv","test_v2.csv"]
    dfMain = dd.read_csv( fileList,
                          dtype={'fullVisitorId': 'str'}, include_path_column=True) 


    dfs = []
    for colname in ['customDimensions']:#, 'hits']:
        dfMain[colname] = dfMain[colname].apply(convertToDict1, meta='object')
        dfs.append(explodeDictCol(dfMain, colname))
        del dfMain[colname]

    
    for colname in ['device', 'geoNetwork', 'totals', 'trafficSource']:
        dfMain[colname] = dfMain[colname].apply(json.loads, meta='object')
        dfs.append(explodeDictCol(dfMain, colname))
        del dfMain[colname]

    dfs.append(dfMain)
    dfMain = dd.concat(dfs, axis='columns')

    del dfMain['hits'] #donno how to use yet
    del dfMain['trafficSource.adwordsClickInfo'] #donno how to use yet

    logger.info('Start computing dataframe')
    dfMain = dfMain.compute()
    logger.info('Done computing dataframe')
    dropConstantCols(dfMain)
    dfMain.to_csv("preprocess.csv.gz", compression='gzip', index=False)