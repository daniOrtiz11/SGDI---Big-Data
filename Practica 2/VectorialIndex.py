"""Sistemas de Gestion de Datos y de la Informacion
   Practica 2 (Mineria de datos y recuperacion de la informacion)
   Grupo 04 - Sergio Gonzalez Francisco / Daniel Ortiz Sanchez
   Sergio Gonzalez Francisco y Daniel Ortiz Sanchez declaramos que esta solucion es fruto
   exclusivamente de nuestro trabajo personal. No hemos sido ayudados por ninguna otra
   persona ni hemos obtenido la solucion de fuentes externas, y tampoco hemos compartido
   nuestra solucion con nadie. Declaramos ademas que no hemos realizado de manera
   deshonesta ninguna otra actividad que pueda mejorar nuestros resultados ni perjudicar los
   resultados de los demas."""

import string
from math import log,sqrt
from os import walk


# Dada una linea de texto, devuelve una lista de palabras no vacias 
# convirtiendo a minusculas y eliminando signos de puntuacion por los extremos
# Ejemplo:
#   > extrae_palabras("Hi! What is your name? John.")
#   ['hi', 'what', 'is', 'your', 'name', 'john']
def extrae_palabras(linea):
  return filter(lambda x: len(x) > 0, 
    map(lambda x: x.lower().strip(string.punctuation), linea.split()))


class VectorialIndex(object):

    def __init__(self, directorio, stop=[]):
        n = 0
        self.indiceVectorial = {}
        self.stoplist = stop
        self.numtoDoc = {}
        indiceFrecuencias = {}
        for (path, ficheros, archivos) in walk(directorio):
            if(len(archivos) > 0):
                for a in archivos:
                    ruta = path+'/'+a
                    self.numtoDoc[n] = ruta
                    archivo = open(ruta,mode='r',encoding='iso-8859-1')
                    lines = archivo.readlines()  
                    for l in lines:
                        ex = extrae_palabras(l)
                        for palabra in ex:
                            if palabra not in self.stoplist:
                                if palabra not in indiceFrecuencias:
                                    self.indiceVectorial[palabra] = []
                                    indiceFrecuencias[palabra] = {}
                                if n not in indiceFrecuencias[palabra]:
                                    indiceFrecuencias[palabra][n] = 1
                                else:
                                    indiceFrecuencias[palabra][n] += 1
                    n += 1 
                    archivo.close
        
        """Empezamos a calcular los pesos"""
        for palabra in indiceFrecuencias:
            for d in indiceFrecuencias[palabra]:
                tf = 0
                if(indiceFrecuencias[palabra][d] >= 1):
                    tf = 1 + log(indiceFrecuencias[palabra][d],2)
                numdocsFrec = len(indiceFrecuencias[palabra])
                idf = log(n/numdocsFrec,2)
                self.indiceVectorial[palabra].append((d,tf*idf))
                
        pass

    def consulta_vectorial(self, consulta, n=3):
        listConsultaFake = extrae_palabras(consulta)
        listCopy = extrae_palabras(consulta)
        listConsulta = []
        for l in listConsultaFake:
            listConsulta.append(l)
            
            
        for i in listCopy:
            if i in self.stoplist:
                listConsulta.remove(i)
    
        qvector = [] 
        for i in listConsulta:
            if(i in self.indiceVectorial):
                qvector.append(1)
            else:
                qvector.append(0)
        qvectormod = 0
        for aux in qvector:
            if(aux == 1):
                qvectormod += 1
        qvectormod = sqrt(qvectormod)
        sumpesosDoc = {}
        modpesosDoc = {}
        for w in listConsulta:
            if w in self.indiceVectorial:
                for d,p in self.indiceVectorial[w]:
                    if d not in sumpesosDoc:
                        sumpesosDoc[d] = p
                        modpesosDoc[d] = pow(p,2)
                    else:
                        sumpesosDoc[d] += p
                        modpesosDoc[d] += pow(p,2)
        
        for d in modpesosDoc:
            modpesosDoc[d] = sqrt(modpesosDoc[d])
            
        relevancias = {}
        for d in modpesosDoc:
            if(modpesosDoc[d]*qvectormod != 0):
                relevancias[d] = sumpesosDoc[d] / (modpesosDoc[d]*qvectormod)
            else:
                relevancias[d] = 0
        
        dicRelevancias = sorted(relevancias.items(), key=lambda x: x[1], reverse = True)
        
        i = 0;
        result = []
        for d,k in dicRelevancias:
            if(i < n):
                result.append((self.numtoDoc[d],k))
            else:
                break;
            i += 1
        return result
        pass

    def consulta_conjuncion(self, consulta):
        listConsultaFake = extrae_palabras(consulta)
        listCopy = extrae_palabras(consulta)
        listConsulta = []
        for l in listConsultaFake:
            listConsulta.append(l)
            
        for i in listCopy:
            if i in self.stoplist:
                listConsulta.remove(i)
                
        result = []
        inicial = False
        wrong = False
        for w in listConsulta:
            if w in self.indiceVectorial:
                if inicial == False:
                    for d,p in self.indiceVectorial[w]:
                        result.append(d)
                    inicial = True
                else:
                    laux = []
                    for d,p in self.indiceVectorial[w]:
                        laux.append(d)
                    for lr in result:
                        if lr not in laux:
                            result.remove(lr)
            else:
                wrong = True;
        
        resultdoc = []
        if wrong == False:
            for r in result:
                resultdoc.append(self.numtoDoc[r])
            
        return resultdoc
        pass

