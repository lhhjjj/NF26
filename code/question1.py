def generateur_station_coor():
    import csv
    with open("asos2014.txt") as f:
        for r in csv.DictReader(f):
            yield r

def get_station_coor():
      read=generateur_station_coor()
      data={}
      import pickle
      for i in read:
          data[i["station"]]=[float(i["lon"]),float(i["lat"])]
      pickle.dump(data,open('coordonne.pkl','wb'))

def haversine(lon1, lat1, lon2, lat2):
    from math import radians, cos, sin, asin, sqrt
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371
    return c * r * 1000

def get_nearest_station(coord):
    import pickle,sys
    with open('coordonne.pkl', 'rb') as f:
       data=pickle.loads(f.read())
    minDis=sys.maxsize
    nearestStation=""
    for  station in data.keys():
          dist=haversine(data[station][0],data[station][1],coord[0],coord[1])
          if dist<minDis:
              minDis=dist
              nearestStation=station
    return nearestStation

def  get_data_bystation(station,indicateur):
    from cassandra.cluster import Cluster
    session = Cluster(["localhost"])
    db = session.connect("groupe_td2_1718")
    query=f"SELECT station, year, month, day, {indicateur} FROM asos_coor WHERE station='{station}';"
    rows=db.execute(query)
    return rows

def  get_data_bycoor(coord,indicateur):
    station=get_nearest_station(coord)
    from cassandra.cluster import Cluster
    session = Cluster(["localhost"])
    db = session.connect("groupe_td2_1718")
    query=f"SELECT station, year, month, day, {indicateur} FROM asos_coor WHERE station='{station}' ;"
    rows=db.execute(query)
    return rows

def get_data(point,indicateur):
    if type(point)==str:
         return get_data_bystation(point,indicateur)
    else:
         return get_data_bycoor(point,indicateur)

def boxplot(point,indicateur):
    import findspark
    from math import ceil
    findspark.init('/opt/spark')
    from pyspark import SparkContext,SparkConf
    conf=SparkConf().setAppName('PySparkShell').setMaster('local[*]')
    sc=SparkContext.getOrCreate(conf=conf)
    data=get_data(point,indicateur)
    D = sc.parallelize(data).filter(lambda truc: truc[4] is not None)
    map = D.map(lambda data: (ceil(data[2]/3),data[4]))
    group=map.groupByKey().collect()
    dict={}
    for saison in group:
        tmp=[]
        for i in saison[1]:
            if i is not None:
                tmp.append(i)
        dict[saison[0]]=tmp
    for i in range(1,5):
         if i not in dict.keys():
              dict[i]=[]
    import matplotlib.pyplot as plt
    colors=['pink','lightblue','lightgreen','orange']
    f=plt.boxplot([dict[1],dict[2],dict[3],dict[4]],patch_artist=True,showmeans=True,meanline=True)
    for box,color in zip(f['boxes'],colors):
        box.set_facecolor(color)
    plt.grid()
    plt.xlabel("Saison")
    plt.ylabel(f"{indicateur}")
    plt.title(f"The boxplot of {indicateur} at {point}")
    plt.savefig(f'./projet_plots/boxplot_{indicateur}_{point}.png')
    plt.close()
    print("Successful ! The figure is saved.")

def linechartparmois(point,indicateur):
    import findspark
    findspark.init('/opt/spark')
    from pyspark import SparkContext, SparkConf
    import numpy as np
    conf = SparkConf().setAppName('PySparkShell').setMaster('local[*]')
    sc = SparkContext.getOrCreate(conf=conf)
    data = get_data(point, indicateur)
    D = sc.parallelize(data)
    map=D.filter(lambda data: data[4] is not None).map(lambda data: (data[2],np.array([1,data[4],data[4],data[4]])))
    reduce=map.reduceByKey(lambda a,b: (a[0]+b[0],min(a[1],b[1]),max(a[2],b[2]),a[3]+b[3]))
    res=reduce.sortByKey().map(lambda truc: (truc[0],truc[1][1],truc[1][2],truc[1][3]/truc[1][0])).collect()
    x=[i[0] for i in res]
    minimum=[i[1] for i in res]
    maximum=[i[2] for i in res]
    mean=[i[3] for i in res]
    import matplotlib.pyplot as plt
    plt.grid()
    plt.plot(x,minimum,linestyle='-.',marker="^",color="pink",label='min')
    plt.plot(x,maximum,linestyle='--',marker="v",color='lightgreen',label='max')
    plt.plot(x,mean,color='orange',marker="o",label='mean')
    plt.legend()
    plt.xlabel("Month")
    plt.ylabel(f"{indicateur}")
    plt.title(f"The line chart of {indicateur} at {point} by month")
    plt.savefig(f'./projet_plots/linechart_{indicateur}_{point}.png')
    plt.close()
    print("Successful ! The figure is saved.")

def linechart_histoire(point,indicateur):
    import findspark
    findspark.init('/opt/spark')
    from pyspark import SparkContext, SparkConf
    import numpy as np
    conf = SparkConf().setAppName('PySparkShell').setMaster('local[*]')
    sc = SparkContext.getOrCreate(conf=conf)
    data = get_data(point, indicateur)
    D = sc.parallelize(data)
    map=D.filter(lambda data: data[4] is not None).map(lambda data: ((data[1],data[2]),np.array([1,data[4]])))
    reduce=map.reduceByKey(lambda a,b: a+b)
    res=reduce.sortByKey().map(lambda truc: (truc[0],truc[1][1]/truc[1][0]))
    donnes=res.map(lambda truc:(truc[0][0],np.array([truc[0][1],truc[1]]))).groupByKey().collect()
    import matplotlib.pyplot as plt
    colors=['rosybrown','tomato','saddlebrown','yellowgreen','steelblue','darkviolet','lightpink','navy','grey','lightseagreen']
    markers=[".","v","+","o","P","x","s","*","2","d"]
    k=0
    for i in donnes:
        y=[0]*12
        x=[0]*12
        for j in i[1]:
            index=int(j[0]-1)
            y[index]=j[1]
            x[index]=j[0]
        plt.plot(x, y, linestyle='-.', marker=markers[k],color=colors[k], label=f'{i[0]}')
        k=k+1
    plt.grid()
    plt.legend()
    plt.xlabel("Month")
    plt.ylabel(f"{indicateur}")
    plt.title(f"The histoire of {indicateur} on average at {point} ")
    plt.savefig(f'./projet_plots/histoire_{indicateur}_{point}.png',)
    plt.close()
    print("Successful ! The figure is saved.")




