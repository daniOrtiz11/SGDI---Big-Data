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
  
  #Obtenemos un df por episode_id y la suma de las palabras de cada uno
  dfe = df4.groupBy('episode_id').agg(functions.sum(df4.word_count).alias("word_count")).selectExpr("episode_id as id", "word_count")

  #Obtenemos un df con episode_id, suma de palabras y imbd_ratin
  dfjoin1 = df2.join(dfe, on = "id", how = 'outer').select("id","imdb_rating","word_count")

  #Obetenmos un df con episode_id y el numero de dialogos que tiene cada uno
  dfe2 = df4.filter(df4.speaking_line == True).groupBy('episode_id').agg(functions.count(df4.raw_text).alias("raw_text_count")).selectExpr("episode_id as id", "raw_text_count")
  dfjoin2 = dfjoin1.join(dfe2, on = "id", how = 'outer').select("id","imdb_rating","word_count","raw_text_count")
  dfjoin2.show()
  sc.stop()

# Punto de entrada del programa al cargarlo con spark-submit
if __name__ == "__main__":
  main()
