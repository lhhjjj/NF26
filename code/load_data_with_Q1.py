import csv
import re

def loadata(filename):
    dateparser = re.compile(
        "(?P<year>\d+)-(?P<month>\d+)-(?P<day>\d+) (?P<hour>\d+):(?P<minute>\d+)"
    )
    with open(filename) as f:
        for r in csv.DictReader(f):
            match_timestamp = dateparser.match(r["valid"])
            lon=r["lon"]
            lat = r["lat"]
            if not match_timestamp or not lon or not lat:
                continue
            start = match_timestamp.groupdict()
            data = {}
            data["station"] = r["station"]
            data["valid"] = (
                int(start["year"]),
                int(start["month"]),
                int(start["day"]),
                int(start["hour"]),
                int(start["minute"])
            )
            data["lon"] = r["lon"]
            data["lat"] = r["lat"]
            data["tmpf"] = r["tmpf"]
            data["dwpf"] = r["dwpf"]
            data["relh"] = r["relh"]
            data["drct"] = r["drct"]
            data["sknt"] = r["sknt"]
            data["p01i"] = r["p01i"]
            data["alti"] = r["alti"]
            data["mslp"] = r["mslp"]
            data["vsby"] = r["vsby"]
            data["gust"] = r["gust"]
            data["skyc1"] = r["skyc1"]
            data["skyc2"] = r["skyc2"]
            data["skyc3"] = r["skyc3"]
            data["skyc4"] = r["skyc4"]
            data["skyl1"] = r["skyl1"]
            data["skyl2"] = r["skyl2"]
            data["skyl3"] = r["skyl3"]
            data["skyl4"] = r["skyl4"]
            data["wxcodes"] = r["wxcodes"]
            data["ice_accretion_1hr"] = r["ice_accretion_1hr"]
            data["ice_accretion_3hr"] = r["ice_accretion_3hr"]
            data["ice_accretion_6hr"] = r["ice_accretion_6hr"]
            data["peak_wind_gust"] = r["peak_wind_gust"]
            data["peak_wind_drct"] = r["peak_wind_drct"]
            data["peak_wind_time"] = r["peak_wind_time"]
            data["feel"] = r["feel"]
            data["metar"] = r["metar"]
            yield data

def writecdr_bycoordinates(csvfilename):
    from cassandra.cluster import Cluster
    session=Cluster(["localhost"])
    db=session.connect("groupe_td2_1718")
    db.execute("drop table asos_coor;")
    sql_create="""CREATE TABLE asos_coor(station text,year int, month int, day int, hour int, minute int, lon float,lat float,
               tmpf float,dwpf float,relh float,drct float,sknt float,p01i float,alti float,mslp float,vsby float,gust float,
               skyc1 text ,skyc2 text,skyc3 text,skyc4 text,
               skyl1 float,skyl2 float,skyl3 float,skyl4 float,
               wxcodes text, ice_accretion_1hr float,ice_accretion_3hr float,ice_accretion_6hr float,
               peak_wind_gust float, peak_wind_drct float, peak_wind_time float,
               feel float,metar text, PRIMARY KEY((station),year,month,day,hour,minute));"""
    db.execute(sql_create)
    inserted = 0
    for data in loadata(csvfilename):
            sql=f"""INSERT INTO asos_coor (station,year,month,day,hour,minute,lon,lat,tmpf,dwpf,relh,drct,sknt,p01i,alti,mslp,vsby,gust,skyc1,skyc2,skyc3,skyc4,skyl1,skyl2,skyl3,skyl4,
               wxcodes,ice_accretion_1hr,ice_accretion_3hr,ice_accretion_6hr,peak_wind_gust,peak_wind_drct,peak_wind_time,feel,metar) 
               VALUES ('{data["station"]}',{data["valid"][0]},{data["valid"][1]},{data["valid"][2]},{data["valid"][3]},{data["valid"][4]},
              {data["lon"]},{data["lat"]},{data["tmpf"]},
              {data["dwpf"]},{data["relh"]},{data["drct"]},{data["sknt"]},{data["p01i"]},
              {data["alti"]},{data["mslp"]},{data["vsby"]},{data["gust"]},'{data["skyc1"]}',
             '{data["skyc2"]}','{data["skyc3"]}','{data["skyc4"]}',{data["skyl1"]},{data["skyl2"]},
              {data["skyl3"]},{data["skyl4"]},'{data["wxcodes"]}',{data["ice_accretion_1hr"]},{data["ice_accretion_3hr"]},
              {data["ice_accretion_6hr"]},{data["peak_wind_gust"]},{data["peak_wind_drct"]},{data["peak_wind_time"]},{data["feel"]},'{data["metar"]}');"""
            db.execute(sql)
            inserted += 1
    db.shutdown()
    return inserted

