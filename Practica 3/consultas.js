/*Sistemas de Gestion de Datos y de la Informacion
   Practica 3 (MongoDB)
   Grupo 04 - Sergio Gonzalez Francisco / Daniel Ortiz Sanchez
   Sergio Gonzalez Francisco y Daniel Ortiz Sanchez declaramos que esta solucion es fruto
   exclusivamente de nuestro trabajo personal. No hemos sido ayudados por ninguna otra
   persona ni hemos obtenido la solucion de fuentes externas, y tampoco hemos compartido
   nuestra solucion con nadie. Declaramos ademas que no hemos realizado de manera
   deshonesta ninguna otra actividad que pueda mejorar nuestros resultados ni perjudicar los
   resultados de los demas.*/



/* AGGREGATION PIPELINE */
// 1.- 
function agg1(){
	/* */
	//unwind para sacar el pais del array, siempre hay que agrupar por el id.
	db.peliculas.aggregate(
			[
				{$unwind : "$pais"}, 
				{$group: {_id : "$pais", numPeliculas : {$sum : 1}}}, 
				{$sort: {numPeliculas: -1, _id : 1 }}
			]
	)
}

// 2.- 
function agg2(){
  /* */
	db.usuarios.aggregate(
		[
			{$match : {"direccion.pais" : "Emiratos Árabes Unidos"}},
			{$unwind : "$gustos"},
			{$group : {_id: "$gustos", numUsuarios : { $sum : 1}}},
			{$sort: {numUsuarios : -1, _id : 1}},
			{$limit: 3}
		]
	)
}
  
// 3.- 
function agg3(){
  /* */
    //project para que no salga el count realizado.
	db.usuarios.aggregate(
		[
			{$match : {"edad":{"$gt": 17}}},
			{$group : {_id: "$direccion.pais", "min":{"$min":"$edad"},"max":{"$max":"$edad"},"media":{"$avg":"$edad"}, numUsuarios: {$sum:1}}},
			{$match: {numUsuarios:{"$gt": 1}}},
			{$project: {numUsuarios: 0}}
		]
	)
}  
  
  
// 4.- 
function agg4(){
  /* */
	db.usuarios.aggregate(
		[
			{$unwind : "$visualizaciones"},
			{$group : {_id: "$visualizaciones.titulo", numVisualizaciones : { $sum : 1}}},
			{$sort: {numVisualizaciones : -1, _id : 1}},
			{$limit: 10}
		]
	)
}



  
/* MAPREDUCE */  
  
// 1.- 
function mr1(){
	/* */
	
	db.peliculas.mapReduce(
		function(){
			for(var i = 0; i < this.pais.length; i++){
				emit(this.pais[i],1)
			}
		},
		function(key, values){
			return Array.sum(values)
		},
		{
			out: {inline:1}
		}
	)
}

// 2.- 
function mr2(){
	/* */
	
	db.usuarios.mapReduce(
		function(){
			if(this.direccion.pais == "Emiratos Árabes Unidos"){
				for(var i = 0; i < this.gustos.length;i++){
					emit(this.gustos[i],1)
				}
			}
		},
		function(key,values){
			return Array.sum(values)
		},
		{
			out: {inline:1}
		}
	)
}

// 3.- 
function mr3(){
	/* */
	
	db.usuarios.mapReduce(
		function(){
			if(this.edad > 17)
					emit(this.direccion.pais,{"edadMinima":this.edad,"edadMaxima":this.edad,"edadMedia":this.edad})
		},
		function(key,values){
			
			var edadMinima = 99999, edadMaxima = 0, sum = 0, edadMedia = 0;
			for(var i = 0; i < values.length; i++){
				if(values[i].edadMinima < edadMinima)
					edadMinima = values[i].edadMinima
				if(values[i].edadMaxima > edadMaxima)
					edadMaxima = values[i].edadMaxima
				
				sum += values[i].edadMedia
			}
			edadMedia = sum / values.length
			return {edadMinima, edadMaxima, edadMedia}
		},
		{
			out: {inline:1}
		}
	)
	
}

// 4.- 
function mr4(){
	/* */
	
	db.usuarios.mapReduce(
		function(){
				for(var i = 0; i < this.visualizaciones.length;i++){
					emit(this.visualizaciones[i].titulo,1)
				}
		},
		function(key,values){
			return Array.sum(values)
		},
		{
			out: {inline:1}
		}
	)
}

