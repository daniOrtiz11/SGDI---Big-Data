"""Sistemas de Gestion de Datos y de la Informacion
   Practica 1 (MapReduce & Apache Spark)
   Grupo 04 - Sergio Gonzalez Francisco / Daniel Ortiz Sanchez
   Sergio Gonzalez Francisco y Daniel Ortiz Sanchez declaramos que esta solucion es fruto
   exclusivamente de nuestro trabajo personal. No hemos sido ayudados por ninguna otra
   persona ni hemos obtenido la solucion de fuentes externas, y tampoco hemos compartido
   nuestra solucion con nadie. Declaramos ademas que no hemos realizado de manera
   deshonesta ninguna otra actividad que pueda mejorar nuestros resultados ni perjudicar los
   resultados de los demas."""

import sys
from pyspark.sql import SparkSession


def main():
 
  if len(sys.argv) != 3:
    print("Faltan los fichero")
    exit(-1)

  # Creamos un contexto local y cargamos los fichero       
  spark = SparkSession.builder.getOrCreate()
  sc = spark.sparkContext
  file1 = sc.textFile("JCMB_2013.csv")
  file2 = sc.textFile("JCMB_2014.csv")
  
  #Se unen los 2 ficheros en el mismo RDD.
  lines = file1.union(file2)

  # Funcion que nos devuelve parejas de (Fecha, valor) ya formateadas.
  def getRDDOfPairs(values):
     battery = float(values[8])  # Se debe de usar el float para que quite la cabecera de la linea del csv (battery) 
     dates = values[0].split("/")[1] + "/" + values[0].split("/")[0]
     return (dates, battery)

  # Filtramos para que no nos coja la primera linea y conseguimos las parejas (Fecha, valor)
  rdd = (lines.map(lambda x: x.split(','))
  	      .filter(lambda x: x[0]!='date-time')
	      .map(getRDDOfPairs)
	)

  # Una vez se tenga el RDD fecha-> Valor
  # Se agrupa por key y dentro del map se mete ya la cadena formateada de lo que se quiere sacar por pantalla, calculando las respectivas cosas.
  cadena = (rdd.groupByKey()
	       .map(lambda x: (x[0],{'max': max(x[1]),'avg':round(sum(x[1])/len(x[1]),3), 'min': min(x[1])}))	  
	       .collect()
 	   )
 
  #Recorrer para mostrarlos.
  for value in cadena:
     print(value)

  sc.stop()


# Punto de entrada del programa al cargarlo con spark-submit
if __name__ == "__main__":
  main()
