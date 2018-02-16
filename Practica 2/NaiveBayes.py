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
import math
import json

class NaiveBayes(object):

    def __init__(self, fichero, smooth=1):
        with open(fichero) as csvfile:
            reader = csv.DictReader(csvfile)
            fields = reader.fieldnames
            """Diccionario que contendra como claves los campos de la cabecera,
                y como valor otro dicc con los posibles valores y el num de veces que aparece"""
            dicField = {}
            
            """Diccionario que contendra como claves los posibles valores del campo class y 
            como valor otro dicc con los atributos como clave y otro dicc como valor con los valores
            de cualquier atributo y el num de veces 
            que aparece"""
            dicClasses = {}
            self.dicProb = {}
            self.probs = {}
            self.probOnlyClass = {}
            self.instancias = 0
            for i in fields:
                dicField[i] = {}
            for row in reader:
                self.instancias += 1
                if (row["class"] not in dicClasses):
                    dicClasses[row["class"]] = {}
                for k,v in row.items():
                    if (v not in dicField[k]):
                        dicField[k][v] = 1
                    else :
                        dicField[k][v] = dicField[k][v] + 1
                    
                    if(k not in dicClasses[row["class"]]):
                        dicClasses[row["class"]][k] = {}
                    if(v not in dicClasses[row["class"]][k]):
                        dicClasses[row["class"]][k][v] = 1
                    else:
                        dicClasses[row["class"]][k][v] = dicClasses[row["class"]][k][v] + 1
                        
            #Salida por pantalla
            print ("Total instancias: "+str(self.instancias))
            print()
            self.showAttributes(dicField)
            print()
            self.showRelationInst(dicClasses, dicField)
            
            #Calculo de las propabilidades
            self.calculateProb(dicField,smooth)
        pass

    def showAttributes(self,dic):
        for k,v in dic.items():
            k2 = v.keys()
            cadena = str.join(',', k2)
            if(k != "class"):
                print ("Atributo '"+k+"': {"+cadena+"}")
            else:
                print ("Clase: {"+cadena+"}")
                print()
                for kc,vc in v.items():
                    cadenac = "Instancias clase '"+kc+"': "+str(vc)
                    print(cadenac)
        pass
    
    def showRelationInst(self,dic, dicField):
        for k,v in dicField.items():
            if(k != "class"):
                attr = v.keys()
                for a in attr:
                    n = 0
                    for kc, vc in dic.items():
                        if(a in vc[k]):
                            n = vc[k][a]
                        else:
                            n = 0
                        cadena = "Instancias ("+k+" = "+a+", class = "+kc+"): "+str(n)
                        if(k not in self.dicProb):
                            self.dicProb[k] = {}
                        if(a not in self.dicProb[k]):
                            self.dicProb[k][a] = {}
                        if(kc not in self.dicProb[k][a]):
                            self.dicProb[k][a][kc] = {}
                        self.dicProb[k][a][kc] = n
                        print(cadena)
        pass

    def calculateProb(self,dicField,smooth):
        for c1 in dicField["class"].keys():
            if (c1 not in self.probOnlyClass):
                self.probOnlyClass[c1] = {}
            self.probOnlyClass[c1] = (dicField["class"][c1]/(self.instancias))
            
        self.probs = self.dicProb.copy()
        for k,v in dicField.items():
            if(k != "class"):
                for v1 in v.keys():
                    for c1 in dicField["class"].keys():
                        num = (self.dicProb[k][v1][c1] + smooth)
                        den = ((dicField["class"][c1])+(self.instancias*smooth))
                        self.probs[k][v1][c1] = (num/den)
                       
        pass
    
    def clasifica(self, instancia):
        probIns = {}
        
        #Probabilidad de cada clase.
        for c1 in self.probOnlyClass.keys():
            probIns[c1] = math.log(self.probOnlyClass[c1],2)
            
        #Probabilidad de cada valor de la clase para cada valor de cada atributo    
        for a in instancia.keys():
            for c1 in self.probOnlyClass.keys():
                probIns[c1] += math.log(self.probs[a][instancia[a]][c1],2)
         
        #Máximo de todas las probabilidades.
        resul = max(probIns, key=probIns.get)
        return resul
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

