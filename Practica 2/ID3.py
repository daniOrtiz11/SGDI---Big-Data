"""Sistemas de Gestion de Datos y de la Informacion
   Practica 2 (Mineria de datos y recuperacion de la informacion)
   Grupo 04 - Sergio Gonzalez Francisco / Daniel Ortiz Sanchez
   Sergio Gonzalez Francisco y Daniel Ortiz Sanchez declaramos que esta solucion es fruto
   exclusivamente de nuestro trabajo personal. No hemos sido ayudados por ninguna otra
   persona ni hemos obtenido la solucion de fuentes externas, y tampoco hemos compartido
   nuestra solucion con nadie. Declaramos ademas que no hemos realizado de manera
   deshonesta ninguna otra actividad que pueda mejorar nuestros resultados ni perjudicar los
   resultados de los demas."""

import csv
from math import log
import json
class ID3(object):

    def __init__(self, fichero):
        self.tree = {}
        with open(fichero) as csvfile:
            reader = csv.DictReader(csvfile)
            fields = reader.fieldnames
            numrows = 0
            datos = []
            for row in reader:
                 numrows += 1
                 datos.append(row)
            newfields = []
            for f in fields:
                if (f != "class") and (f not in newfields):
                    newfields.append(f)
# diccionario dentro de diccionario, atributo, valor atributo, numrepet clases
        self.dicAttrClass = {}
        for f in newfields:
            if f not in self.dicAttrClass:
                self.dicAttrClass[f] = {}
        for d in datos:
            for f in newfields:
                if d[f] not in self.dicAttrClass[f]:
                    self.dicAttrClass[f][d[f]] = {}
                if d["class"] not in self.dicAttrClass[f][d[f]]:
                    self.dicAttrClass[f][d[f]][d["class"]] = 1
                else:
                    self.dicAttrClass[f][d[f]][d["class"]] += 1
       
            self.tree = self.tdidt(datos, numrows ,newfields)
        pass

    def tdidt(self, datos, longdatos, attrCandidatos):
        dicClases = {}
        for d in datos:
            if d["class"] not in dicClases:
                dicClases[d["class"]] = 1
            else:
                dicClases[d["class"]] += 1
# cp := clase que aparece mas veces en C
        clasemas = ''
        repmas = 0
        for k,v in dicClases.items():
            if clasemas == '':
                clasemas = k
                repmas = v
            else:
                if repmas < v:
                  repmas = v
                  clasemas = k       
# si todas las instancias en C son de clase cp entonces
#si l es vacía entonces
        if repmas == longdatos or len(attrCandidatos) == 0:
            return {'tipo':'hoja','clase':clasemas}
        
        a = self.selecciona_atributo(datos,longdatos, attrCandidatos,self.dicAttrClass)
        particiones = {}
        
        for fila in datos:
             val = fila[a] 
             if val in particiones:
                particiones[val].append(fila)
             else:
                particiones[val] = []
                particiones[val].append(fila)
        nohoja = {'tipo':'nohoja','atributo':a,'hijos':{}}
        newattrCandidatos = [ai for ai in attrCandidatos if ai != a]

        
        for v in self.dicAttrClass[a]:	
            h = {}
            if v not in particiones:
                 h = {'tipo':'hoja','clase':clasemas}
            else:
                 h = self.tdidt(particiones[v],len(particiones[v]),newattrCandidatos)
            nohoja['hijos'][v] = h
        return nohoja
        pass
    
    def selecciona_atributo(self, datos, longdatos, atributos,dicAtributosClass):
         listaentropia = []
         dicentropia = {}
         dicvalentropia = {}
         for a in atributos:
             dicentropia[a] = 0
             for v in dicAtributosClass[a]:
                  tam = 0
                  for c,vc in dicAtributosClass[a][v].items():
                      tam += vc
                  for c in dicAtributosClass[a][v]:
                      frac = dicAtributosClass[a][v][c]/tam
                      if v not in dicvalentropia:
                          dicvalentropia[v] = 0
                      dicvalentropia[v] -= frac * log(frac,2)
                  dicentropia[a]  += (tam/longdatos)*(dicvalentropia[v]) 
             listaentropia.append((a, dicentropia[a]))
         amin = ''
         vmin = 0
         ini = False;
         for a,v in listaentropia:
             if ini == False:
                 vmin = v
                 amin = a
             else:
                if v < vmin:
                    vmin = v
                    amin = a
         return amin
         
         pass
    def clasifica(self, instancia):
        return self.clasifica_arbol(self.tree, instancia)        
        pass
    
    def clasifica_arbol(self,tree,instancia):
        if tree['tipo'] == 'hoja':
             
	        return tree['clase']
		
        else:
            for atrib,val in instancia.items():
	            if atrib == tree['atributo']:
                     nAux = {a:v for a,v in instancia.items() if a != atrib}
                     return self.clasifica_arbol(tree['hijos'][val], nAux)
        pass
        
    def test(self, fichero):
        with open(fichero) as csvfile:
            print("Test:")
            aciertos,total = 0,0
            reader = csv.DictReader(csvfile)
            resulrow,predict = '',''
            for row in reader:
                dic = {}
                for k,v in row.items():
                    if(k != "class"):
                        dic[k] = v
                    else:
                        resulrow = v
                cadenadic = json.dumps(dic)
                print(cadenadic+"-"+resulrow)
                predict = self.clasifica(dic)
                print("Clase predicha: "+predict)
                print()
                if(predict == resulrow):
                    aciertos += 1
                total += 1
        return (aciertos,total,aciertos/total)
        pass


    def save_tree(self, fichero):
        self.it = 0
        self.nodes = []
        self.aristas = []
	
        #Función para recorrer el árbol e ir guardando sus nodos/aristas.
        self.dibujarArbol(self.tree, None, None)
	
        #Recorremos los nodos y aristas anteriores para irlos escribiendo en el archivo.
        output_file = open(fichero,'w+')
        
        output_file.write('digraph tree {\n') 
        
        #Nodos
        output_file.write('//nodos')
        output_file.write('\n')
        for node in self.nodes:
            output_file.write(node)
            output_file.write('\n')
        
        #Aristas
        output_file.write('//aristas')
        output_file.write('\n')
        for arista in self.aristas:
            output_file.write(arista)
            output_file.write('\n')

        output_file.write('}')
        output_file.close()
        pass
    
    def dibujarArbol(self, tree, nodeP, valP):
        
        #Si se ha llegado a la hoja
        if(tree['tipo'] == 'hoja'):       
            className = tree['clase'].replace(' ','_')
            classId = className + str(self.it)
            
            self.nodes.append(classId +' [label="' + className +'"];')
            self.aristas.append(nodeP + ' -> '+ classId + '[label="'+ valP .title() +'"];')
            
            return
        else:
            attributeName = tree['atributo'].replace(' ','_')
            nodeId = attributeName + str(self.it)
            
            self.nodes.append(nodeId + ' [label="'+ attributeName.title() + '", shape="box"];')
            
            if (self.it > 0):
                self.aristas.append(nodeP + ' -> '+ nodeId +'[label="'+ valP.title() +'"];')

            for val in tree['hijos']:
                self.it += 1
                self.dibujarArbol(tree['hijos'][val],nodeId, val)
        pass


