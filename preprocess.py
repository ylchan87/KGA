import numpy as np
import dask
import dask.dataframe as dd
import dask.bag as db
import pandas as pd
import json
import logging
from dask.diagnostics import Profiler, ResourceProfiler, CacheProfiler
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

hitsFields = {
    "type_PAGEcnt" : 0 , 
    "type_TRANSACTIONcnt" : 0 , 
    "type_ITEMcnt" : 0 , 
    "type_EVENTcnt" : 0 , 
    "type_SOCIALcnt" : 0 , 
    "type_APPVIEWcnt" : 0 ,
    "eCommerceAction_type0cnt": 0,
    "eCommerceAction_type1cnt": 0,
    "eCommerceAction_type2cnt": 0,
    "eCommerceAction_type3cnt": 0,
    "eCommerceAction_type4cnt": 0,
    "eCommerceAction_type5cnt": 0,
    "eCommerceAction_type6cnt": 0,
    "eCommerceAction_type7cnt": 0,
    "pagePathLevel1_Rank1" : "",
    "pagePathLevel1_Rank1cnt" : 0,
    "pagePathLevel1_Rank2" : "",
    "pagePathLevel1_Rank2cnt" : 0,
    "pagePathLevel2_Rank1" : "",
    "pagePathLevel2_Rank1cnt" : 0,
    "pagePathLevel2_Rank2" : "",
    "pagePathLevel2_Rank2cnt" : 0,
    "pagePathLevel3_Rank1" : "",
    "pagePathLevel3_Rank1cnt" : 0,
    "pagePathLevel3_Rank2" : "",
    "pagePathLevel3_Rank2cnt" : 0,
    "pagePathLevel4_Rank1" : "",
    "pagePathLevel4_Rank1cnt" : 0,
    "pagePathLevel4_Rank2" : "",
    "pagePathLevel4_Rank2cnt" : 0,
    "contentGroup1_Rank1" : "",
    "contentGroup1_Rank1cnt" : 0,
    "contentGroup1_Rank2" : "",
    "contentGroup1_Rank2cnt" : 0,
    "contentGroup2_Rank1" : "",
    "contentGroup2_Rank1cnt" : 0,
    "contentGroup2_Rank2" : "",
    "contentGroup2_Rank2cnt" : 0,
    "contentGroup3_Rank1" : "",
    "contentGroup3_Rank1cnt" : 0,
    "contentGroup3_Rank2" : "",
    "contentGroup3_Rank2cnt" : 0,
    "contentGroup4_Rank1" : "",
    "contentGroup4_Rank1cnt" : 0,
    "contentGroup4_Rank2" : "",
    "contentGroup4_Rank2cnt" : 0,
    "contentGroup5_Rank1" : "",
    "contentGroup5_Rank1cnt" : 0,
    "contentGroup5_Rank2" : "",
    "contentGroup5_Rank2cnt" : 0,
    "favProduct1_SKU" : "",
    "favProduct1_Category" : "",
    "favProduct1_Price" : 0,
    "favProduct1_cnt" : 0,
    "favProduct2_SKU" : "",
    "favProduct2_Category" : "",
    "favProduct2_Price" : 0,
    "favProduct2_cnt" : 0,
}

def flattenHits(hits):
    hits = eval(hits)
    output = hitsFields.copy()

    pagePathLevel = [[] for i in range(4)]
    contentGroup = [[] for i in range(5)]

    favProducts = {}
    def addToFavProducts(aProd):
        try:
            favProducts[aProd['productSKU']]['cnt']+=1
        except KeyError:
            favProducts[aProd['productSKU']] = {
                'cnt' : 1,
                'Category' : aProd['v2ProductCategory'],
                'Price': aProd['productPrice'],
            }
    
    for hit in hits:
        try:
            tmp = hit['type']
            output['type_%scnt'%tmp]+=1
        except KeyError:
            pass
        
        try:
            tmp = hit['eCommerceAction']['action_type']
            output['eCommerceAction_type%scnt'%tmp]+=1
        except KeyError:
            pass

        try:
            pageRec = hit['page']
            for i in range(4):
                tmp = pageRec.get('pagePathLevel%s'%(i+1))
                if tmp: pagePathLevel[i].append(tmp)
        except KeyError:
            pass

        try:
            gpRec = hit['contentGroup']
            for i in range(5):
                tmp = gpRec.get('contentGroup%s'%(i+1))
                if tmp and tmp!='(not set)': contentGroup[i].append(tmp)
        except KeyError:
            pass

        try:
            prodRec = hit['product']
            if len(prodRec)==1:
                addToFavProducts(prodRec[0])
            else:
                for aProd in prodRec:
                    if aProd.get('isClick')==True: addToFavProducts(aProd)
        except KeyError:
            pass
        
    #find the most frequent entries in page, contentGroup and products
    for i in range(4):
        strings, cnts = np.unique(pagePathLevel[i], return_counts=True)
        ranked = list(zip(cnts,strings))
        ranked.sort(reverse=True)
        for r in range(2):
            if r>=len(ranked): break
            output["pagePathLevel%s_Rank%s"%(i+1,r+1)] = ranked[r][1]
            output["pagePathLevel%s_Rank%scnt"%(i+1,r+1)] = ranked[r][0]

    for i in range(5):
        strings, cnts = np.unique(contentGroup[i], return_counts=True)
        ranked = list(zip(cnts,strings))
        ranked.sort(reverse=True)
        for r in range(2):
            if r>=len(ranked): break
            output["contentGroup%s_Rank%s"%(i+1,r+1)] = ranked[r][1]
            output["contentGroup%s_Rank%scnt"%(i+1,r+1)] = ranked[r][0]
    
    ranked = sorted(favProducts.items(), key=lambda kv:kv[1]['cnt'], reverse=True)
    for r in range(2):
        if r>=len(ranked): break
        output["favProduct%s_SKU"     %(r+1)] = ranked[r][0]
        output["favProduct%s_Category"%(r+1)] = ranked[r][1]['Category']
        output["favProduct%s_Price"   %(r+1)] = ranked[r][1]['Price']
        output["favProduct%s_cnt"     %(r+1)] = ranked[r][1]['cnt']

    return output
    
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
            'sessionQualityDim',
            'timeOnSite',
            'totalTransactionRevenue', 
            'transactionRevenue',
            'transactions', 
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
        'hits': hitsFields.keys(),
    }

    #this explode a column of dicts to columns of its keys
    
    #dfOut = df[colname].apply(convertList,dtype='object')
    # tmp = [(c,'object') for c in fields.get(colname)]
    # dfOut = df[colname].to_bag().to_dataframe(meta=tmp)
    
    for subcolname in fields.get(colname): 
        df["%s.%s"%(colname,subcolname)] = df[colname].apply(lambda d, k=subcolname: d.get(k), meta='object')
        # df["%s.%s"%(colname,subcolname)] = dfOut[subcolname]

    
    del df[colname]
    

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

    # dfMain = pd.read_csv( fileList[0],
    #                       dtype={'fullVisitorId': 'str'}) 

    for colname in ['hits']:
        dfMain[colname] = dfMain[colname].apply(flattenHits, meta='dict')
        explodeDictCol(dfMain, colname)

    for colname in ['customDimensions']:#, 'hits']:
        dfMain[colname] = dfMain[colname].apply(convertToDict1, meta='dict')
        explodeDictCol(dfMain, colname)
    
    for colname in ['device', 'geoNetwork', 'totals', 'trafficSource']:
        dfMain[colname] = dfMain[colname].apply(json.loads, meta='dict')
        explodeDictCol(dfMain, colname)
        
    del dfMain['trafficSource.adwordsClickInfo'] #donno how to use yet

    logger.info('Start computing dataframe')

    with Profiler() as prof, ResourceProfiler(dt=0.25) as rprof, CacheProfiler() as cprof:
        #1 worker is faster on my multi-core machine, maybe due to sequential disk read?
        dfMain_c = dfMain.compute(num_workers=1)

    
    logger.info('Done computing dataframe')
    dropConstantCols(dfMain_c)
    dfMain_c.to_csv("preprocess.csv.gz", compression='gzip', index=False)