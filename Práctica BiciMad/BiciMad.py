#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
import json
from pprint import pprint


def datos(df1,df2,df3):
    d1 = spark.read.json(df1)
    d2 = spark.read.json(df2)
    d3 = spark.read.json(df3)
    df1 = d1.drop(*('_corrupt_record', 'track','idplug_base','idunplug_base')).na.drop("all")
    df2 = d2.drop(*('_corrupt_record', 'track','idplug_base','idunplug_base')).na.drop("all")
    df3 = d3.drop(*('_corrupt_record', 'track','idplug_base','idunplug_base')).na.drop("all")
    return df1.union(df2.union(df3))


invierno = datos('201801_Usage_Bicimad.json','201802_Usage_Bicimad.json','201812_Usage_Bicimad.json')
primavera = datos('201803_Usage_Bicimad.json','201804_Usage_Bicimad.json','201805_Usage_Bicimad.json')
verano = datos('201806_Usage_Bicimad.json','201807_Usage_Bicimad.json','201808_Usage_Bicimad.json')
otoño = datos('201809_Usage_Bicimad.json','201810_Usage_Bicimad.json','201811_Usage_Bicimad.json')


print ('Invierno:')
invierno.printSchema()

print ('Primavera:')
primavera.printSchema()

print ('Verano:')
verano.printSchema()

print ('Otoño:')
otoño.printSchema()

print ('En invierno el número de viajes fue: ' + str(invierno.count()))
print ('En primavera el número de viajes fue: ' + str(primavera.count()))
print ('En verano el número de viajes fue: ' + str(verano.count()))
print ('En otoño el número de viajes fue: ' + str(otoño.count()))


def meses(df):
    m = spark.read.json(df)
    mes = m.drop(*('_corrupt_record', 'track','idplug_base','idunplug_base')).na.drop("all")
    return mes


marzo = meses('201803_Usage_Bicimad.json')
abril = meses('201804_Usage_Bicimad.json')
mayo = meses('201805_Usage_Bicimad.json')


print ('Marzo:')
marzo.groupBy('user_type').sum('travel_time').show()

print ('Abril:')
abril.groupBy('user_type').sum('travel_time').show()

print ('Mayo:')
mayo.groupBy('user_type').sum('travel_time').show()


print ('En marzo el número de viajes fue: ' + str(marzo.count()))
print ('En abril el número de viajes fue: ' + str(abril.count()))
print ('En mayo el número de viajes fue: ' + str(mayo.count()))


marzo19 = meses('201903_Usage_Bicimad.json')
abril19 = meses('201904_Usage_Bicimad.json')
mayo19 = meses('201905_Usage_Bicimad.json')


print ('En marzo el número de viajes fue: ' + str(marzo19.count()))
print ('En abril el número de viajes fue: ' + str(abril19.count()))
print ('En mayo el número de viajes fue: ' + str(mayo19.count()))


print ('Verano:')
verano.groupBy('user_type').sum('travel_time').show()
print ('Otoño:')
otoño.groupBy('user_type').sum('travel_time').show()



print ('Invierno:')
invierno.groupBy('user_type').sum('travel_time').show()
print ('Primavera:')
primavera.groupBy('user_type').sum('travel_time').show()


junio = meses('201806_Usage_Bicimad.json')
julio = meses('201807_Usage_Bicimad.json')
agosto = meses('201808_Usage_Bicimad.json')


print ('Junio:')
junio.groupBy('user_type').sum('travel_time').show()

print ('Julio:')
julio.groupBy('user_type').sum('travel_time').show()

print ('Agosto:')
agosto.groupBy('user_type').sum('travel_time').show()



print ('En junio el número de viajes fue: ' + str(junio.count()))
print ('En julio el número de viajes fue: ' + str(julio.count()))
print ('En agosto el número de viajes fue: ' + str(agosto.count()))



septiembre = meses('201809_Usage_Bicimad.json')
octubre = meses('201810_Usage_Bicimad.json')
noviembre = meses('201811_Usage_Bicimad.json')


print ('Septiembre:')
septiembre.groupBy('user_type').sum('travel_time').show()

print ('Octubre:')
octubre.groupBy('user_type').sum('travel_time').show()

print ('Noviembre:')
noviembre.groupBy('user_type').sum('travel_time').show()


print ('En septiembre el número de viajes fue: ' + str(septiembre.count()))
print ('En noviembre el número de viajes fue: ' + str(octubre.count()))
print ('En octubre el número de viajes fue: ' + str(noviembre.count()))


diciembre = meses('201812_Usage_Bicimad.json')
enero = meses('201801_Usage_Bicimad.json')
febrero = meses('201802_Usage_Bicimad.json')


print ('Diciembre:')
diciembre.groupBy('user_type').sum('travel_time').show()

print ('Enero:')
enero.groupBy('user_type').sum('travel_time').show()

print ('Febrero:')
febrero.groupBy('user_type').sum('travel_time').show()


print ('En diciembre el número de viajes fue: ' + str(diciembre.count()))
print ('En enero el número de viajes fue: ' + str(enero.count()))
print ('En febrero el número de viajes fue: ' + str(febrero.count()))


print ('Marzo:')
marzo.groupBy('user_type').sum('travel_time').show()

print ('Abril:')
abril.groupBy('user_type').sum('travel_time').show()

print ('Mayo:')
mayo.groupBy('user_type').sum('travel_time').show()


verano19 = datos('201906_Usage_Bicimad.json','201907_movements.json','201908_movements.json')
print ('Verano19:')
verano19.groupBy('user_type').sum('travel_time').show()

primavera19 = datos('201903_Usage_Bicimad.json','201904_Usage_Bicimad.json','201905_Usage_Bicimad.json')
print ('Primavera19:')
primavera19.groupBy('user_type').sum('travel_time').show()


verano17 = datos('201706_Usage_Bicimad.json','201707_Usage_Bicimad.json','201708_Usage_Bicimad.json')
print ('Verano17:')
verano17.groupBy('user_type').sum('travel_time').show()

otoño17 = datos('201709_Usage_Bicimad.json','201710_Usage_Bicimad.json','201711_Usage_Bicimad.json')
print ('Otoño17:')
otoño17.groupBy('user_type').sum('travel_time').show()


print ('Junio:')
pgj = junio.groupBy('idunplug_station').count()
pgj.filter(pgj['count']>3500).show()   

print ('Julio:')
pgjl = julio.groupBy('idunplug_station').count()
pgjl.filter(pgjl['count']>3500).show()

print ('Agosto:')
pga = agosto.groupBy('idunplug_station').count()
pga.filter(pga['count']>3500).show()


print ('Septiembre:')
pgs = septiembre.groupBy('idunplug_station').count()
pgs.filter(pgs['count']>3500).show()  

print ('Octubre:')
pgo = octubre.groupBy('idunplug_station').count()
pgo.filter(pgo['count']>3500).show()

print ('Noviembre:')
pgn = noviembre.groupBy('idunplug_station').count()
pgn.filter(pgn['count']>3500).show()


print ('Junio:')
pgj = junio.groupBy('idplug_station').count()
pgj.filter(pgj['count']>3500).show()   

print ('Julio:')
pgjl = julio.groupBy('idplug_station').count()
pgjl.filter(pgjl['count']>3500).show()

print ('Agosto:')
pga = agosto.groupBy('idplug_station').count()
pga.filter(pga['count']>3500).show()


print ('Septiembre:')
pgs = septiembre.groupBy('idplug_station').count()
pgs.filter(pgs['count']>3500).show()  

print ('Octubre:')
pgo = octubre.groupBy('idplug_station').count()
pgo.filter(pgo['count']>3500).show()

print ('Noviembre:')
pgn = noviembre.groupBy('idplug_station').count()
pgn.filter(pgn['count']>3500).show()



print ('Junio:')
pgj = junio.groupBy('idunplug_station').count()
pgj.filter(pgj['count']<500).show()   

print ('Julio:')
pgjl = julio.groupBy('idunplug_station').count()
pgjl.filter(pgjl['count']<500).show()

print ('Agosto:')
pga = agosto.groupBy('idunplug_station').count()
pga.filter(pga['count']<500).show()



print ('Septiembre:')
pgs = septiembre.groupBy('idunplug_station').count()
pgs.filter(pgs['count']<500).show()  

print ('Octubre:')
pgo = octubre.groupBy('idunplug_station').count()
pgo.filter(pgo['count']<500).show()

print ('Noviembre:')
pgn = noviembre.groupBy('idunplug_station').count()
pgn.filter(pgn['count']<500).show()



print ('Junio:')
pgj = junio.groupBy('idplug_station').count()
pgj.filter(pgj['count']<500).show()   

print ('Julio:')
pgjl = julio.groupBy('idplug_station').count()
pgjl.filter(pgjl['count']<500).show()

print ('Agosto:')
pga = agosto.groupBy('idplug_station').count()
pga.filter(pga['count']<500).show()



print ('Septiembre:')
pgs = septiembre.groupBy('idplug_station').count()
pgs.filter(pgs['count']<500).show()  

print ('Octubre:')
pgo = octubre.groupBy('idplug_station').count()
pgo.filter(pgo['count']<500).show()

print ('Noviembre:')
pgn = noviembre.groupBy('idplug_station').count()
pgn.filter(pgn['count']<500).show()



print ('Junio:')
junio.groupBy('ageRange').count().show()

print ('Julio:')
julio.groupBy('ageRange').count().show()

print ('Agosto:')
agosto.groupBy('ageRange').count().show()


print ('Septiembre:')
septiembre.groupBy('ageRange').count().show()

print ('Octubre:')
octubre.groupBy('ageRange').count().show()

print ('Noviembre:')
noviembre.groupBy('ageRange').count().show()



print ('Diciembre:')
diciembre.groupBy('ageRange').count().show()

print ('Enero:')
enero.groupBy('ageRange').count().show()

print ('Febrero:')
febrero.groupBy('ageRange').count().show()


print ('Marzo:')
marzo.groupBy('ageRange').count().show()

print ('Abril:')
abril.groupBy('ageRange').count().show()

print ('Mayo:')
mayo.groupBy('ageRange').count().show()

