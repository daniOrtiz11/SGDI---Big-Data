"""Sistemas de Gestion de Datos y de la Informacion
   Practica 3 (MongoDB)
   Grupo 04 - Sergio Gonzalez Francisco / Daniel Ortiz Sanchez
   Sergio Gonzalez Francisco y Daniel Ortiz Sanchez declaramos que esta solucion es fruto
   exclusivamente de nuestro trabajo personal. No hemos sido ayudados por ninguna otra
   persona ni hemos obtenido la solucion de fuentes externas, y tampoco hemos compartido
   nuestra solucion con nadie. Declaramos ademas que no hemos realizado de manera
   deshonesta ninguna otra actividad que pueda mejorar nuestros resultados ni perjudicar los
   resultados de los demas."""

# -*- coding: utf-8 -*-

import pymongo
import pprint
from pymongo import MongoClient
from bson.objectid import ObjectId

client = MongoClient('localhost',27017)
db = client["sgdi_pr3"]
#peliculas = db["peliculas"]
#usuarios = db["usuarios"]

# 1. Fecha y título de las primeras 'n' peliculas vistas por el usuario 'user_id'
# usuario_peliculas( 'fernandonoguera', 3 )
def usuario_peliculas(user_id, n):
    #db.usuarios.find({_id:user_id},{_id:0, "visualizaciones.titulo":1, "visualizaciones.fecha":1, visualizaciones:{$slice:n}})
    return db["usuarios"].find({"_id":user_id},{"_id":0, "visualizaciones.titulo":1, "visualizaciones.fecha":1, "visualizaciones":{"$slice":n}})
    
   
# 2. _id, nombre y apellidos de los primeros 'n' usuarios a los que les gusten 
# varios tipos de película 'gustos' a la vez
# usuarios_gustos(  ['terror', 'comedia'], 5  )
def usuarios_gustos(gustos, n):
    #db.usuarios.find({"gustos":{"$all":["terror", "comedia"]}},{"nombre":1, "apellido1":1, "apellido2":1}).limit(5)
    return db["usuarios"].find({"gustos":{"$all": gustos}},{"nombre":1, "apellido1":1, "apellido2":1}).limit(n)
    

  
# 3. _id de usuario de sexo 'sexo' y con una edad entre 'edad_min' e 'edad_max' incluidas
# usuario_sexo_edad('M', 50, 80)
def usuario_sexo_edad( sexo, edad_min, edad_max ):
    return db["usuarios"].find({"sexo":sexo, "edad":{"$gte":edad_min, "$lte":edad_max}},{})	
    
# 4. Nombre, apellido1 y apellido2 de los usuarios cuyos apellidos coinciden,
#    ordenado por edad ascendente
# usuarios_apellidos()
def usuarios_apellidos():
    return db["usuarios"].find({"$where":"return this['apellido1'] == this['apellido2'];"},{"nombre":1, "apellido1":1, "apellido2":1}).sort("edad",1)	
    
    
# 5.- Titulo de peliculas cuyo director empiece con un 'prefijo' dado
# pelicula_prefijo( 'Yol' )
def pelicula_prefijo( prefijo ):
    return db["peliculas"].find({"director":{"$regex":"^"+prefijo}},{"_id":0,"titulo":1})
    

# 6.- _id de usuarios con exactamente 'n' gustos, ordenados por edad descendente
# usuarios_gustos_numero( 6 )
def usuarios_gustos_numero(n):
    return db["usuarios"].find({ "$where": "this.gustos.length == "+str(n) },{}).sort("edad",-1)	
    
    
# 7.- usuarios que vieron pelicula la pelicula 'id_pelicula' en un periodo 
#     concreto 'inicio' - 'fin'
# usuarios_vieron_pelicula( '583ef650323e9572e2812680', '2015-01-01', '2016-12-31' )
def usuarios_vieron_pelicula(id_pelicula, inicio, fin):
    newid = ObjectId(id_pelicula)
    return db["usuarios"].find({"visualizaciones":{"$elemMatch":{"_id":newid, "fecha":{"$gte":inicio, "$lte":fin}}}},{})
	
"""usuario_peliculas( 'fernandonoguera', 3 )
usuarios_gustos(['terror', 'comedia'], 5 )
usuario_sexo_edad('M', 50, 80)
usuarios_apellidos()
pelicula_prefijo( 'Yol' )
usuarios_gustos_numero( 6 )
usuarios_vieron_pelicula( '583ef650323e9572e2812680', '2015-01-01', '2016-12-31' )"""