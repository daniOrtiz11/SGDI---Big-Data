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

  # Cargamos los ficheros csv directamente como dataframes      
  spark = SparkSession.builder.getOrCreate()
  sc = spark.sparkContext
  df1 = spark.read.format("csv").option("header", "true").load("simpsons_characters.csv")
  df2 = spark.read.format("csv").option("header", "true").load("simpsons_episodes.csv")
  df3 = spark.read.format("csv").option("header", "true").load("simpsons_locations.csv")
  df4 = spark.read.format("csv").option("header", "true").load("simpsons_script_lines.csv")
  #Mostramos los dataframes cargados anteriormente
  df1.show()
  df2.show()
  df3.show()
  df4.show()

  sc.stop()

# Punto de entrada del programa al cargarlo con spark-submit
if __name__ == "__main__":
  main()
