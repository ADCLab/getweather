import os
import pandas
import geopy.distance
# from functools import lru_cache
from methodtools import lru_cache
from sshtunnel import SSHTunnelForwarder
import pymysql
from datetime import datetime
from numpy import where, interp 
from numpy import array, zeros, hstack
from numpy import nanmax, nan
from numpy import sin, cos, arctan2, pi
from math import isnan

skycDict={'VV':5,'OVC':4,'BKN':3,'SCT':2,'FEW':1,'CLR':0}

def getStationLocations(conn):
    if os.path.exists('stationLocations.csv'):
        return pandas.read_csv('stationLocations.csv')
    else:
        myq='Select distinct station,lat,lon from stationLocations;'
        return pandas.read_sql(myq,conn)

def weightAvgWindDrct(angs,weights):
    sAngleX=sum([w*cos(x*pi/180) for x,w in zip(angs,weights)])/sum(weights)
    sAngleY=sum([w*sin(x*pi/180) for x,w in zip(angs,weights)])/sum(weights)
    tmpAngle=arctan2(sAngleY,sAngleX)*180/pi
    return (tmpAngle>=0)*tmpAngle+(tmpAngle<0)*(360+tmpAngle)    

def interpWeatherAtStation(weatherData,epochTime):
    iWeatherStation=[]
    if len(weatherData)<5:
        print('Not enough data to interpolate at station')
        return -1
    else:
        try:
            for cField in ['tmpf','dwpf','relh','p01i','alti','vsby','skyc1','skyc2','skyc3','skyc4','skyl1','skyl2','skyl3','skyl4','sknt','wgust','wgustmax']:
                xy=weatherData[['valid',cField]].dropna().values
                iWeatherStation.append(interp(epochTime, xy[:,0], xy[:,1], left=None, right=None, period=None))
            
            sIdx=weatherData['valid'].le(epochTime).idxmax()
            eIdx=weatherData['valid'].ge(epochTime).idxmax()
            angs=[weatherData.at[sIdx,'drct'],weatherData.at[eIdx,'drct']]
            alpha=(weatherData.at[eIdx,'valid']-epochTime)/(weatherData.at[eIdx,'valid']-weatherData.at[sIdx,'valid'])
            weights=[alpha,1-alpha]
            iWeatherStation.append(  weightAvgWindDrct(angs,weights)  )
        except:
            print('Error on interpolation')
            return -1
    return iWeatherStation
    
# @lru_cache(maxsize = 100)
# def stationData(station,windowCenter,windowSize):
#     windowLeft=windowCenter-windowSize
#     windowRight=windowCenter+windowSize
#     myq='Select station,lat,lon from fullASOS where ;'
#     return pandas.read_sql(myq,conn)    

class WeatherInterp(object):
    
    def __init__(self,hostIP,ruser,rpw,myuser,mypw,myport):
        self.stations=pandas.read_csv('stationLocations.csv')
        self.myStationList=[]
        self.tunnel = SSHTunnelForwarder(
            hostIP,
            ssh_username=ruser,
            ssh_password=rpw,
            local_bind_address=('127.0.0.1', myport),
            remote_bind_address=('127.0.0.1', 3306))   
        self.tunnel.start()

        self.conn= pymysql.connect(user=myuser,
                                          password=mypw,
                                          host='127.0.0.1',
                                          db='asos',
                                          port=myport)
        
        self.cWindows=dict()

    # def getWeatherAtLocation(self,atLat,atLon,atDateTime,atHalfWindow=60,maxDist=20):
    #     stationList,weightsDist=nearestStations(round(atLat,3),round(atLon,3),maxDist)
    #     stationWeather, weightsTime=zip(*[self.getStationWeather(cStation,atDateTime,atHalfWindow) for cStation in stationList])
    #     finalWeights=[wDist*wTime for wDist,wTime in zip(weightsDist,weightsTime)]
    #     finalWeights=[w/sum(finalWeights) for w in finalWeights]
    #     return stationWeather.dot(finalWeights)
        

    

        
    @lru_cache(maxsize = 100)
    def nearestStations(self,lat,lon,maxDist,N=20):
        stationsLocal=self.stations.copy()
        stationsLocal['dist']=[geopy.distance.great_circle((lat, lon),(sLat,sLon)).miles for sLat,sLon in zip(stationsLocal['lat'],stationsLocal['lon'])]
        stationsLocal=stationsLocal[stationsLocal['dist']<maxDist]
        stationsLocal.sort_values('dist',inplace=True)
        density=self.getStationDensity(stationsLocal['station'])
        years=density['year'].unique()
        for index, row in stationsLocal.iterrows():   
            for cYear in years:
                try:
                    cDensity=density[(density['station']==row['station']) & (density['year']==cYear)]['density'].values[0]
                except:
                    cDensity=None
                stationsLocal.at[index,cYear]=cDensity

        return stationsLocal.sort_values('dist')

    def setMyStationsInNeighborhoodAuto(self,lat,lon,maxDist,N=20):
        stationsLocal=self.nearestStations(lat,lon,maxDist,N)
        if len(stationsLocal)>0:
            stationNames=stationsLocal['station'].tolist()
            stationWeights=((1/stationsLocal['dist'])/sum(1/stationsLocal['dist'])).tolist()
            self.setMyStations(stationNames,stationWeights)
        else:
            print('No stations within %0.2f'%maxDist)

    def getStationDensity(self,stationNames):
        if isinstance(stationNames, str):
            stationNames=[stationNames]
        myq_whereStation='('+' or '.join(['station="%s"'%cStation for cStation in stationNames])+')'
        myq='select station,year(valid) as year,24*60*datediff(max(valid),min(valid))/count(*) as density from fullASOS where %s group by station,year(valid);'%myq_whereStation
        return pandas.read_sql(myq,self.conn)
    
    def setMyStations(self,stationNames,stationWeights):
        if isinstance(stationNames, str):
            stationNames=[stationNames]
        self.myStationList=stationNames
        self.myStationWeights=stationWeights
        for cStation in stationNames:
            self.cWindows[cStation]=[]
        
    
    def getInterpWeatherAtMyStations(self,epochTime,inWindow='left'):
        if inWindow=='left':
            dtLeft=65*60 
            dtRight=24*65*60
        elif inWindow=='center':
            dtLeft=12*65*60
            dtRight=12*65*60
        elif inWindow=='right':
            dtLeft=24*65*60
            dtRight=65*60

        weatherData=dict()            
        iWeather=dict()
        for cStation in self.myStationList:
            checkInside=[(cWindow-dtLeft+65*60<=epochTime) & (epochTime<=cWindow+dtRight-65*60)   for cWindow in self.cWindows[cStation]]
            if any(checkInside):
                idx=where(checkInside)[0][0]
                useCenterWindow=self.cWindows[cStation][idx]
            else:
                self.cWindows[cStation].append(epochTime)
                useCenterWindow=epochTime
            # print(cStation)
            weatherData[cStation]=self.getStationWeatherData(cStation,useCenterWindow,dtLeft,dtRight)
            weatherData[cStation]['gust'].fillna(value=nan, inplace=True)
            weatherData[cStation]['peak_wind_gust'].fillna(value=nan, inplace=True)
            weatherData[cStation]['wgust']=nanmax(weatherData[cStation][['sknt','gust']].values,axis=1)
            weatherData[cStation]['wgustmax']=nanmax(weatherData[cStation][['wgust','peak_wind_gust']].values,axis=1)
            for cField in ['skyc1','skyc2','skyc3','skyc4']:
                weatherData[cStation][cField]=[skycDict.get(x,0) for x in weatherData[cStation][cField]]
            for cField in ['skyl1','skyl2','skyl3','skyl4']:
                weatherData[cStation][cField].fillna(value=25000, inplace=True)
            iWeather[cStation]=interpWeatherAtStation(weatherData[cStation],epochTime)
        
        return iWeather

    def getWeatherAtTime(self,epochTime,inWindow='left'):
        iWeather=self.getInterpWeatherAtMyStations(epochTime,inWindow)
        drctWeights=[]
        drctAngs=[]
        sWeights=0
        sWeather=zeros(17)
        for cStation, cWeight in zip(self.myStationList,self.myStationWeights):
            if iWeather[cStation] != -1:
                sWeights=sWeights+cWeight            
                sWeather=sWeather+cWeight*array(iWeather[cStation][:-1])
                if not isnan(iWeather[cStation][-1]):
                    drctAngs.append(iWeather[cStation][-1])
                    drctWeights.append(cWeight)
        idrct=weightAvgWindDrct(drctAngs,drctWeights) 
        wValues=hstack((sWeather/sWeights,idrct))
        wKeys=['tmpf','dwpf','relh','p01i','alti','vsby','skyc1','skyc2','skyc3','skyc4','skyl1','skyl2','skyl3','skyl4','sknt','wgust','wgustmax','wdrct']
        return dict(zip(wKeys, wValues)),wValues
    
    

        
    @lru_cache(maxsize = 100)
    def getStationWeatherData(self,cStation,cWindow,dtLeft,dtRight):
        DateTimeLower=datetime.utcfromtimestamp(cWindow-dtLeft).strftime('%Y-%m-%d %H:%M:%S')
        DateTimeUpper=datetime.utcfromtimestamp(cWindow+dtRight).strftime('%Y-%m-%d %H:%M:%S')
        myq='Select * from fullASOS where station="%s" and valid>="%s" and valid<="%s";'%(cStation,DateTimeLower,DateTimeUpper)
        asosDF=pandas.read_sql(myq,self.conn)   
        asosDF['valid']=asosDF['valid'].astype('int64')//1e9
        return asosDF


         
        
        