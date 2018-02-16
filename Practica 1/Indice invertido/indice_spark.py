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

  # Creamos un contexto local y cargamos los ficheros       
  spark = SparkSession.builder.getOrCreate()
  sc = spark.sparkContext
  
  # Se cogen todos los libros de la carpeta books para meterlos en el mismo RDD con cabecera de que pertenece a cada libro.
  lines = sc.wholeTextFiles("books")

  # Se aplana el texto y se consiguen parejas de ese tipo (Palabra.Archivo, 1).
  def getWordsInBooks(values):
     pairs = []
     fileN = values[0].split('/')[len(values[0].split('/')) - 1]
     lines = formatLines(values[1])
     for word in lines.lower().split():
        pairs.append(((word + "." + fileN), 1))
     return pairs

  # Formateo de los textos.
  def formatLines(values):
     return (''.join(l for l in values if l.isalpha() or l == "'" or l == ' '))
	
  # Se cambia el nombre del archivo, para que quede como key solo la plabra.	
  def changeNameFile(values):	 
     key = values[0].split(".")
     return (key[0], (key[1],values[1]))

  # Se comprueba si algun count es mayor que 20.	 
  def moreThan20(values): 
     ok = False;
     for count in values[1]:
        if(count[1] > 20):
           ok = True   
     return ok

  # Ordena el resultado final, es necesario poner el reverse para que quede ordenado de mayor a menor.
  def sortCounts(values):
     return (values[0], sorted(values[1], key=lambda values: values[1], reverse=True))

  # Formateo de la salida.
  def formatOut(values):
     return (', '.join(str((book,count)) for book,count in values))
	

  # Primero formateamos el texto y se consiguen parejas.		
  wordsInBooks = lines.flatMap(getWordsInBooks)
  
  # Reducimos por Key, dejamos la palabra como Key unica y agrupamos por la palabra.
  countWordsInBooks = (wordsInBooks.reduceByKey(lambda x,y: x+y)
				   .map(changeNameFile)
				   .groupByKey()
		      )
  # Aplicamos el filtro de si en algun libro esa palabra est mas de 20 veces y lo ordenamos.
  filterWordsInBooks = countWordsInBooks.filter(moreThan20)
  sortWordsCount = filterWordsInBooks.map(sortCounts)

  # Se recupera el resultado con collect.
  sortCollect = sortWordsCount.collect()

  #Mostramos los resultados.
  for value in sortCollect:
     countsBooks = formatOut(value[1])
     print (str(value[0] + countsBooks))
  sc.stop()


# Punto de entrada del programa al cargarlo con spark-submit
if __name__ == "__main__":
  main()
