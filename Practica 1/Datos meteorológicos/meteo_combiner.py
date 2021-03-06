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


class MRDatosMeteo(MRJob):

  # Fase MAP (line es una cadena de texto)
  def mapper(self, key, line):
    spl = line.split(',')
    dateHour = spl[0]
    spl2 = dateHour.split()
    date = spl2[0]
    spl3 = date.split('/')
    longspl3 = len(spl3)
    if (longspl3 > 1):
       keytoYield = spl3[1] +"/"+spl3[0]
       dic = {}
       dic[spl[8]] = 1
       yield keytoYield, dic

  # Fase Combiner (key es una cadena texto, values un generador de valores)
  def combiner(self,key,values):
    sumL = 0
    dic = {}
    for d in values:
       for k,v in d.items():
          if k in dic:
             dic[k] = dic[k] + 1
          else:
             dic[k] = 1
    yield key, dic
     
  # Fase REDUCE (key es una cadena texto, values un generador de valores)
  def reducer(self, key, values):
    maxV = 0
    minV = 9223372036854775807
    sumV = 0
    longv = 0
    for d in values:
      for k,v in d.items():  
        k = float(k)
        if(k > maxV):
          maxV = k
        if(k < minV):
          minV = k 
        while i < v:
          sumV = sumV + k
          longv = longv + 1
          i = i + 1
    mediaV = round(sumV / longv, 3)
    yield key, ('max:'+str(maxV), 'avg:'+str(mediaV), 'min:'+str(minV))


if __name__ == '__main__':
    MRDatosMeteo.run()
