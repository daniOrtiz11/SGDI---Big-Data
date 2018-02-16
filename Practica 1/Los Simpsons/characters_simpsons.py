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
  
  # Obtenemos un df por episode_id y el count de los distintos personajes.
  dfe = df4.groupBy('episode_id').agg(functions.approx_count_distinct(df4.character_id).alias("character_count")).selectExpr("episode_id as id", "character_count")

  # Obtenemos un df con id, imbd_rating y el count de personajes.
  dfjoin1 = df2.join(dfe, on = "id", how = 'outer').select("id","imdb_rating","character_count")

  # Obtenemos (id, genero) e (id, episodio) y los unimos por los id's de los personajes.
  dfc = df1.select('id','gender').selectExpr("id as character_id", "gender")
  dfe = df4.select('character_id','episode_id')
  dfjoin2 = dfc.join(dfe, on = "character_id", how = 'outer').select("episode_id", "character_id","gender")

  # Para obtener el count de los generos, hacemos un filtro y agrupamos por id de episodio, para después hacer el count de cada episodio.
  dfCharacMasc =  dfjoin2.filter(dfjoin2.gender == 'm').groupBy('episode_id').agg(functions.approx_count_distinct(dfjoin2.character_id).alias("masc_count")).selectExpr("episode_id as id", "masc_count")
  dfCharacFem =  dfjoin2.filter(dfjoin2.gender == 'f').groupBy('episode_id').agg(functions.approx_count_distinct(dfjoin2.character_id).alias("fem_count")).selectExpr("episode_id as id", "fem_count")

  # Por último se unen todos los campos en un mismo dataframe y se muestra.
  dfjoin3 = dfjoin1.join(dfCharacMasc, on = "id", how = 'outer').select("id","imdb_rating","character_count", "masc_count")
  dfjoin4 = dfjoin3.join(dfCharacFem, on = "id", how = 'outer').select("id","imdb_rating","character_count", "masc_count", "fem_count")
  
  dfjoin4.show()
  sc.stop()

# Punto de entrada del programa al cargarlo con spark-submit
if __name__ == "__main__":
  main()
