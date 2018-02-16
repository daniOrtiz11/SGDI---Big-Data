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
from pyspark.sql import functions



def main():

  # Cargamos los ficheros csv directamente como dataframes      
  spark = SparkSession.builder.getOrCreate()
  sc = spark.sparkContext
  df1 = spark.read.format("csv").option("header", "true").load("simpsons_characters.csv")
  df2 = spark.read.format("csv").option("header", "true").load("simpsons_episodes.csv")
  df3 = spark.read.format("csv").option("header", "true").load("simpsons_locations.csv")
  df4 = spark.read.format("csv").option("header", "true").load("simpsons_script_lines.csv")

  dfScriptLines = df4.groupBy('episode_id').agg(functions.approx_count_distinct(df4.location_id).alias("count")).selectExpr("episode_id as id", "count")

  dfLocations = df2.join(dfScriptLines, on = "id", how = 'outer').select("id","imdb_rating","count")

  dfLocations.show()

  sc.stop()

# Punto de entrada del programa al cargarlo con spark-submit
if __name__ == "__main__":
  main()
