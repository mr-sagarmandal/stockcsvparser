from pandas import DataFrame, read_csv
import pandas as pd

Location= r'C:\Users\sagar\Documents\QuantlabExcercise\input.csv'

def csvParseCustom(directory):
    store={};
    df= pd.read_csv(Location, names=['Time','Company', 'Stocks', 'Value'], header=None, chunksize=10000 )#Chunksize can be changed to optimize
    for chunk in df:
        companies= chunk['Company'].unique()
        for x in companies:
            mask=(chunk['Company']==x)
            timeArray=chunk[mask]['Time'].values.tolist();
            if (len(timeArray)!=1):
                maxTimeDifference=max([abs(timeArray[i+1]-timeArray[i])for i in range(len(timeArray)-1)])
            else:
                maxTimeDifference=0
            stocksXValue= (chunk[mask].Stocks*chunk[mask].Value).values.tolist()
            if x in store:
                store[x]['weightedAverage']=( (store[x]['weightedAverage']) * (store[x]['volume'])+(sum(stocksXValue)))/(store[x]['volume']+chunk[mask]['Stocks'].sum())
                store[x]['volume']=store[x]['volume']+chunk[mask]['Stocks'].sum()
                if (store[x]['timeDiff']<maxTimeDifference):
                    store[x]['timeDiff']= maxTimeDifference
                if ((((chunk[mask]['Time'].min()-store[x]['hiTime'])))>store[x]['timeDiff']):
                    store[x]['timeDiff']= ((chunk[mask]['Time'].min()-store[x]['hiTime']))
                if (store[x]['maxVal']<chunk[mask]['Value'].max()):
                    store[x]['maxVal']=chunk[mask]['Value'].max()

                store[x]['hiTime']=chunk[mask]['Time'].max()
            else:
                store[x]={};
                store[x]['timeDiff']= maxTimeDifference
                store[x]['maxVal']=chunk[mask]['Value'].max()
                store[x]['hiTime']=chunk[mask]['Time'].max()
                store[x]['volume']=chunk[mask]['Stocks'].sum()
                store[x]['weightedAverage']=((sum(stocksXValue))/chunk[mask]['Stocks'].sum())

    for keys in store:
        store[keys]['weightedAverage']=int(store[keys]['weightedAverage'])
        del(store[keys]['hiTime'])
    return (store)

def toCSV(mappedData):
    data=pd.DataFrame.from_dict(mappedData, orient='index')
    data.to_csv('output.csv',header= False,columns=['timeDiff','volume','weightedAverage','maxVal'])

x= csvParseCustom(Location)
y= toCSV(x)
