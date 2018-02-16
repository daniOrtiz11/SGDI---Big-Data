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
from pyspark.sql.types import DoubleType   


def main():

  # Cargamos los ficheros csv directamente como dataframes      
  spark = SparkSession.builder.getOrCreate()
  sc = spark.sparkContext
  df1 = spark.read.format("csv").option("header", "true").load("simpsons_characters.csv")
  df2 = spark.read.format("csv").option("header", "true").load("simpsons_episodes.csv")
  df3 = spark.read.format("csv").option("header", "true").load("simpsons_locations.csv")
  df4 = spark.read.format("csv").option("header", "true").load("simpsons_script_lines.csv")
 

  #A) numero de ubicaciones diferentes que aparecen en cada episodio
  dfScriptLines = df4.groupBy('episode_id').agg(functions.approx_count_distinct(df4.location_id).alias("count")).selectExpr("episode_id as id", "count")
  dfLocations = df2.join(dfScriptLines, on = "id", how = 'outer').select("id","imdb_rating","count")  
  dfaux1 = dfLocations.withColumn("imdb_rating_double",dfLocations["imdb_rating"].cast(DoubleType()))  
  dfaux2 = dfaux1.withColumn("count_double",dfLocations["count"].cast(DoubleType()))  
  dfcof = dfaux2.agg(functions.corr(dfaux2.imdb_rating_double, dfaux2.count_double).alias('Pearson-Imdb-NumLocations'))
  dfcof.show()  

  #B) numero de personajes que aparecen en cada episodio
  dfe = df4.groupBy('episode_id').agg(functions.approx_count_distinct(df4.character_id).alias("character_count")).selectExpr("episode_id as id", "character_count")
  dfjoin1 = df2.join(dfe, on = "id", how = 'outer').select("id","imdb_rating","character_count")
  dfaux1 = dfjoin1.withColumn("imdb_rating_double",dfLocations["imdb_rating"].cast(DoubleType()))  
  dfaux2 = dfaux1.withColumn("character_count_double",dfaux1["character_count"].cast(DoubleType()))  
  dfcof = dfaux2.agg(functions.corr(dfaux2.imdb_rating_double, dfaux2.character_count_double).alias('Pearson-Imdb-NumCharacters'))
  dfcof.show()  

  #C y D) numero de personajes masculinos y femeninos que aparecen en cada episodio
  dfc = df1.select('id','gender').selectExpr("id as character_id", "gender")
  dfe = df4.select('character_id','episode_id')
  dfjoin2 = dfc.join(dfe, on = "character_id", how = 'outer').select("episode_id", "character_id","gender")
  dfCharacMasc =  dfjoin2.filter(dfjoin2.gender == 'm').groupBy('episode_id').agg(functions.approx_count_distinct(dfjoin2.character_id).alias("masc_count")).selectExpr("episode_id as id", "masc_count")
  dfCharacFem =  dfjoin2.filter(dfjoin2.gender == 'f').groupBy('episode_id').agg(functions.approx_count_distinct(dfjoin2.character_id).alias("fem_count")).selectExpr("episode_id as id", "fem_count")
  dfjoin3 = dfjoin1.join(dfCharacMasc, on = "id", how = 'outer').select("id","imdb_rating","character_count", "masc_count")
  dfjoin4 = dfjoin3.join(dfCharacFem, on = "id", how = 'outer').select("id","imdb_rating","character_count", "masc_count", "fem_count")
  dfaux1 = dfjoin4.withColumn("imdb_rating_double",dfjoin4["imdb_rating"].cast(DoubleType()))  
  dfaux2 = dfaux1.withColumn("masc_count_double",dfaux1["masc_count"].cast(DoubleType()))  
  dfcof = dfaux2.agg(functions.corr(dfaux2.imdb_rating_double, dfaux2.masc_count_double).alias('Pearson-Imdb-NumCharactersMasc'))
  dfcof.show()  
  dfaux1 = dfjoin4.withColumn("imdb_rating_double",dfjoin4["imdb_rating"].cast(DoubleType()))  
  dfaux2 = dfaux1.withColumn("fem_count_double",dfaux1["fem_count"].cast(DoubleType()))  
  dfcof = dfaux2.agg(functions.corr(dfaux2.imdb_rating_double, dfaux2.fem_count_double).alias('Pearson-Imdb-NumCharactersFem'))
  dfcof.show()  
 

  #E) numero de palabras
  dfe = df4.groupBy('episode_id').agg(functions.sum(df4.word_count).alias("word_count")).selectExpr("episode_id as id", "word_count")
  dfjoin1 = df2.join(dfe, on = "id", how = 'outer').select("id","imdb_rating","word_count")
  dfaux1 = dfjoin1.withColumn("imdb_rating_double",dfjoin1["imdb_rating"].cast(DoubleType()))  
  dfaux2 = dfaux1.withColumn("word_count_double",dfaux1["word_count"].cast(DoubleType()))  
  dfcof = dfaux2.agg(functions.corr(dfaux2.imdb_rating_double, dfaux2.word_count_double).alias('Pearson-Imdb-NumWords'))
  dfcof.show()

  #F) cantidad total de diálogos
  dfe2 = df4.filter(df4.speaking_line == True).groupBy('episode_id').agg(functions.count(df4.raw_text).alias("raw_text_count")).selectExpr("episode_id as id", "raw_text_count")
  dfjoin2 = dfjoin1.join(dfe2, on = "id", how = 'outer').select("id","imdb_rating","word_count","raw_text_count")
  dfaux1 = dfjoin2.withColumn("imdb_rating_double",dfjoin1["imdb_rating"].cast(DoubleType()))  
  dfaux2 = dfaux1.withColumn("raw_text_count_double",dfaux1["raw_text_count"].cast(DoubleType()))  
  dfcof = dfaux2.agg(functions.corr(dfaux2.imdb_rating_double, dfaux2.raw_text_count_double).alias('Pearson-Imdb-NumRawText'))
  dfcof.show()
  
  sc.stop()
# Punto de entrada del programa al cargarlo con spark-submit
if __name__ == "__main__":
  main()