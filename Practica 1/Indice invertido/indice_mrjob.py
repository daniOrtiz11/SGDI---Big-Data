"""Sistemas de Gestion de Datos y de la Informacion
   Practica 1 (MapReduce & Apache Spark)
   Grupo 04 - Sergio Gonzalez Francisco / Daniel Ortiz Sanchez
   Sergio Gonzalez Francisco y Daniel Ortiz Sanchez declaramos que esta solucion es fruto
   exclusivamente de nuestro trabajo personal. No hemos sido ayudados por ninguna otra
   persona ni hemos obtenido la solucion de fuentes externas, y tampoco hemos compartido
   nuestra solucion con nadie. Declaramos ademas que no hemos realizado de manera
   deshonesta ninguna otra actividad que pueda mejorar nuestros resultados ni perjudicar los
   resultados de los demas."""

from mrjob.job import MRJob
import os
import string


class MRDatosMeteo(MRJob):

  # Fase MAP (line es una cadena de texto)
  def mapper(self, key, line):
     line = line.lower() 
     file_name = os.environ['map_input_file']
     for palabra in line.split():
        for a in string.punctuation:
           palabra = palabra.replace(a, '')
        if(palabra != " " or palabra != ""):
           yield palabra, file_name

  # Fase REDUCE (key es una cadena texto, values un generador de valores)
  def reducer(self, key, values):
     #contadores que identifican cuantas veces ha llegado cada libro en values 
     contadventures = 0
     conthamlet = 0
     contmoby = 0
     for e in values:
        if(e == 'Adventures_of_Huckleberry_Finn.txt'):
           contadventures = contadventures + 1;
        if(e == 'Hamlet.txt'):
           conthamlet = conthamlet + 1;
        if(e == 'Moby_Dick.txt'):
           contmoby = contmoby + 1;
     if(conthamlet > 20 or contadventures > 20 or contmoby > 20):
       dic = {}
       if(contmoby != 0):
          dic["Moby_Dick.txt"] = contmoby    
       if(contadventures != 0):
          dic['Adventures_of_Huckleberry_Finn.txt'] = contadventures   
       if(conthamlet != 0):
          dic['Hamlet.txt'] = conthamlet   
       #pasamos diccionario a lista para ordenar por valor
       #pasamos diccionario a lista para ordenar por valor
       l = []
       for k, v in dic.items():
          temp = [k,v]
          l.append(temp)
       l.sort(reverse = True, key=lambda e: e[1])
       yield key, l

if __name__ == '__main__':
    MRDatosMeteo.run()
