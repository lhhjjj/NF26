import sys
import numpy as np
import pandas as pd
from random import sample
import findspark
from pyspark import SparkContext, SparkConf
import copy
import folium
from itertools import chain

def  get_data_bysperiode(start,end):
    from cassandra.cluster import Cluster
    session = Cluster(["localhost"])
    db = session.connect("groupe_td2_1718")
    date_index = pd.date_range(start, end)
    res=iter([])
    for day in date_index:
        query=f"""SELECT  station,lon,lat,
                                            tmpf,dwpf,relh,drct,sknt,alti,vsby ,skyl1,feel
                                            from asos_day
                                            WHERE timestamp_day='{day}';"""
        rows=db.execute(query)
        res=chain(res,rows)
    return res


def get_mean_variable(start,end):
    findspark.init('/opt/spark')
    conf = SparkConf().setAppName('PySparkShell').setMaster('local[*]')
    sc = SparkContext.getOrCreate(conf=conf)
    data = get_data_bysperiode(start, end)
    D= sc.parallelize(data).filter(lambda truc: len([ i is not None for i in truc])==sum([ i is not None for i in truc]))
    map=D.map(lambda data : ((data[0],round(data[1],2),round(data[2],2)),[np.array([1 for i in data[1:]]),np.array([i for i in data[1:]])]))
    reduce=map.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))
    mean=reduce.map(lambda truc: (truc[0],truc[1][1]/truc[1][0] ))
    return mean.collect()

def distance(p1,p2):
    tmp=np.sum((np.array(p1)-np.array(p2))**2)
    return np.sqrt(tmp)

def mymap(point):
    minDis=sys.maxsize
    index=-1
    for i in range(0,len(centers)):
          dis=distance(point,centers[i])
          if dis<minDis:
              minDis=dis
              index= i
    return (index,[1,np.array(point)])

def Kmeans(data, max,k,eps):
     findspark.init('/opt/spark')
     conf = SparkConf().setAppName('PySparkShell').setMaster('local[*]')
     sc = SparkContext.getOrCreate(conf=conf)
     D = sc.parallelize(data)
     global centers
     centers=sample(data,k)
     iteration,tmp=0,sys.maxsize
     while iteration<max and tmp > eps :
           tmp=0
           map1=D.map(mymap)
           reduce=map1.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1]))
           new_centers=reduce.map(lambda truc: (truc[0],truc[1][1]/truc[1][0])).collect()
           centers_copy=copy.deepcopy(centers)
           for i in new_centers:
               centers[int(i[0])]=i[1]
           for i in range(0,k):
              tmp=sum((centers_copy[i]-centers[i])**2)+tmp
           tmp=np.sqrt(tmp)
           iteration=iteration+1
     res=[]
     for i in data:
        for j in map1.collect():
            if all(i==j[1][1]):
                res.append(j[0])
                break
     return res

def normalisation(data):
    min=np.min(data,axis=0)
    max = np.max(data, axis=0)
    max_min=max-min
    for i in range(0,len(max_min)):
        if max_min[i]==0:
            max_min[i]=1
    for i in range(0,len(data)):
        data[i]=(data[i]-min)/max_min
    return data


def clusterisation(start,end,k):
    data=get_mean_variable(start,end)
    stations=[]
    vecteur=[]
    for name,instance in data:
        stations.append(name)
        vecteur.append(np.array(instance))
    labels=Kmeans(normalisation(vecteur),100000,k,0.0001)
    res=[]
    for i,j in zip(stations,labels):
        res.append([i,j])
    return res

def mapofclusterisation(start,end,k=10):
      res=clusterisation(start,end,k)
      colors =['pink', 'cadetblue' , 'black', 'green','beige','orange','lightgrayblack','purple' ,'darkgreen','gray', 'blue', 'darkpurple', 'lightgreen', 'red', 'lightblue']
      france_map = folium.Map(location=[46.42,2.43], zoom_start=6,titles='Clusterisation')
      for station, label in res:
          lon=float(station[1])
          lat=float(station[2])
          folium.Marker([lat, lon], popup=station[0],icon=folium.Icon(color=colors[int(label)])).add_to(france_map)
      france_map.save(f'./projet_plots/france_{start}_{end}.html')
      print("The file of html is saved!")
      return res