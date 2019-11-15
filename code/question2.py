#!python3
# -*- coding: utf-8 -*-
"""
Created on Wed June 05 16:02:29 2019

@author: groupe_td2_1718
"""

#  QUESTION 1
# • À un instant donné je veux pouvoir obtenir une carte me représentant
# n’importe quel indicateur.

import cassandra
import cassandra.cluster
import os
import matplotlib.pyplot as plt 
import seaborn as sns
import glob
import numpy as np
import pandas as pd
import geopandas as gpd
import pykrige.kriging_tools as kt
from pykrige.ok import OrdinaryKriging
from pykrige.uk import UniversalKriging
from mpl_toolkits.basemap import Basemap
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import Path, PathPatch

session = cassandra.cluster.Cluster(['localhost']).connect('groupe_td2_1718')

def db_query(query):
      rows = session.execute(query)
      return rows

# • À un instant donné je veux pouvoir obtenir une carte me représentant
#     n’importe quel indicateur.
def get_data_for_an_hour(year, month, day, hour):
      query = f"""
            SELECT station, year, month, day, hour, lon, lat,
                  tmpf, dwpf, relh, drct, sknt, p01i, alti, mslp, vsby, gust,
                  skyl1, skyl2, skyl3, skyl4,
                  ice_accretion_1hr, ice_accretion_3hr, ice_accretion_6hr,
                  peak_wind_gust, peak_wind_drct, peak_wind_time, feel
            FROM groupe_td2_1718.asos_datetime
            WHERE year = {year}
            AND month = {month}
            AND day = {day}
            AND hour = {hour}
      """
      rows = db_query(query)

      info = dict(
            {
                  'datetime': dict(
                        {
                              'year': year,
                              'month': month,
                              'day': day,
                              'hour': hour
                        }
                  ),
                  # data is an array with the rows fetched, one dict with row info = one element
                  'data' : []
            }
      )

      if not rows :
            print(f"\nNo data registered for {year}-{month}-{day}-{hour}")
      else:
            for data in rows:
                  dico = dict(
                        {
                              'station': data.station,
                              'lon': data.lon,
                              'lat': data.lat,
                              'tmpf': data.tmpf,
                              'dwpf': data.dwpf,
                              'relh': data.relh,
                              'drct': data.drct,
                              'sknt': data.sknt,
                              'p01i': data.p01i,
                              'alti': data.alti,
                              'mslp': data.mslp,
                              'vsby': data.vsby,
                              'gust': data.gust,
                              'skyl1': data.skyl1,
                              'skyl2': data.skyl2,
                              'skyl3': data.skyl3,
                              'skyl4': data.skyl4,
                              'ice_accretion_1hr': data.ice_accretion_1hr,
                              'ice_accretion_3hr': data.ice_accretion_3hr,
                              'ice_accretion_6hr': data.ice_accretion_6hr,
                              'peak_wind_gust': data.peak_wind_gust,
                              'peak_wind_drct': data.peak_wind_drct,
                              'peak_wind_time': data.peak_wind_time,
                              'feel': data.feel,
                        }
                  )
                  info['data'].append(dico)
            return info



# It draws a heatmap for a specific hour and a specific indicator
# It uses kriging to evaluate the values between the known points
def get_plot_per_hour_and_indicator(year, month, day, hour, indicator):
      filename_core = f"{year}-{month}-{day}-{hour}h_{indicator}"
      filename_asc = f"./projet_plots/krige_{filename_core}.asc"

      plt.close('all')
      plt.clf()

      info = get_data_for_an_hour(year, month, day, hour)

      # format data for kriging process
      # array of (lat, long, value)    
      #  OK stands for Ordinary Kriging   
      entry_data_for_OK = []
      lon = []
      lat = []
      min_indic = 1000
      max_indic = 0
      for data in info['data']:
            if data[indicator] is not None:
                  entry_data_for_OK.append(
                        [
                              data['lon'],
                              data['lat'],
                              data[indicator]
                        ]
                  )
                  lon.append(data['lon'])
                  lat.append(data['lat'])
                  if data[indicator] > max_indic:
                        max_indic = data[indicator]
                  if data[indicator] < min_indic:
                        min_indic = data[indicator]
      np_entry_data_for_OK = np.array(entry_data_for_OK)

      print("\n ---> Basic data ready")

      # creates the grid
      min_lon = -5.0
      min_lat = 39.0
      max_lon = 9.0
      max_lat = 53.0
      grid_precision = 200.0
      dx = round((max_lon - min_lon) / grid_precision, 2) 
      dy = round((max_lat - min_lat) / grid_precision, 2)
      gridx = np.arange(min_lon, max_lon, dx)
      gridy = np.arange(min_lat, max_lat, dy)

      z, data_frame = execute_Ordinary_Kriging(np_entry_data_for_OK, gridx, gridy, filename_asc)
      
      xintrp, yintrp = np.meshgrid(gridx, gridy)
      fix, ax = plt.subplots(figsize=(15,15))
      maps = Basemap(llcrnrlon=min_lon,llcrnrlat=min_lat,urcrnrlon=max_lon,urcrnrlat=max_lat, projection='merc', resolution='h',area_thresh=1000.,ax=ax )
      maps.drawcountries()
      maps.drawcoastlines()
      x,y = maps(xintrp, yintrp)
      cs = ax.contourf(x, y, z, np.linspace(min_indic, max_indic, 100),extend='both',cmap='jet')
      cbar = maps.colorbar(cs,location='right',pad="7%")
      plt.title(f'Indicateur : {indicator}\n {year}-{month}-{day} (yyyy-mm-dd) à {hour} heures (France)', fontsize=20)

      plt.savefig(f'./projet_plots/heatmap_{filename_core}.png')

def execute_Ordinary_Kriging(data, gridx, gridy, filename_to_write):
      # Create the ordinary kriging object. Required inputs are the X-coordinates of
      # the data points, the Y-coordinates of the data points, and the Z-values of the
      # data points. If no variogram model is specified, defaults to a linear variogram
      # model. If no variogram model parameters are specified, then the code automatically
      # calculates the parameters by fitting the variogram model to the binned
      # experimental semivariogram. The verbose kwarg controls code talk-back, and
      # the enable_plotting kwarg controls the display of the semivariogram.
      OK = OrdinaryKriging(data[:, 0], data[:, 1],data[:, 2], variogram_model='gaussian',
                        verbose=True, enable_plotting=True)

      # Creates the kriged grid and the variance grid. Allows for kriging on a rectangular
      # grid of points, on a masked rectangular grid of points, or with arbitrary points.
      # (See OrdinaryKriging.__doc__ for more information.)
      z, ss = OK.execute('grid', gridx, gridy)

      # Writes the kriged grid to an ASCII grid file.
      kt.write_asc_grid(gridx, gridy, z, filename=filename_to_write)
      print("\n ---> Kriging terminated, and results loaded")
      ascii_grid = np.loadtxt(filename_to_write, skiprows=7)

      data_frame = pd.DataFrame(ascii_grid, columns=gridx, index=gridy)
      print("\n ---> data_frame created")

      return z, data_frame

     
# Cette liste comprend uniquement le territoire formé par
# la partie métropolitaine continentale de la France (à l'exclusion des îles) :
#     nord : Bray-Dunes, Nord (51° 05′ 21″ N, 2° 32′ 43″ E) ;
#     est : Lauterbourg, Bas-Rhin (48° 58′ 02″ N, 8° 13′ 50″ E) ;
#     sud : Puig de Coma Negra, Lamanère, Pyrénées-Orientales (42° 19′ 58″ N, 2° 31′ 58″ E) ;
#     ouest : pointe de Corsen, Plouarzel, Finistère (48° 24′ 46″ N, 4° 47′ 44″ O).