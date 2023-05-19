import sys
from pyspark import SparkContext
sc = SparkContext()

"""
Estructuras auxiliares:
    Cuando se nombra tupla1, esta es un par cuya primera componente presenta uno y la segunda su adyacencia.

    Cuando se nombra tupla2, esta es un par cuya primera componente presenta dos nodos y la segunda una lista con 
las conexiones entre ellos.

Funciones:
    mapper: limpia el grafo, es decir, ordena los pares de nodos para que siempre sea (menor, mayor) y no considera las aristas que
    vayan de un nodo en si mismo.

    conexiones: dada una tupla 1 devuelve las correspondientes tuplas2.

    posible_triciclo: con esta funcion se "descartan" los casos en los que no haya relación directa o haya menos de dos conexiones.

    triciclo: se aplica despues de posible_triciclo y devuelve los triciclos.

    ejercicio_dos: la función que hace lo que pedía el enunciado, juntando todos los archivos en un RDD.
"""

def mapper(line):
    edge = line.strip().split(',')
    n1 = edge[0]
    n2 = edge[1]
    if n1 < n2:
         return (n1,n2)
    elif n1 > n2:
         return (n2,n1)
    else:
        pass 
        

def conexiones(tupla1):
    lista_conexiones = []
    for i in range(len(tupla1[1])): 
        lista_conexiones.append(((tupla1[0],tupla1[1][i]),'exists')) 
        for j in range(i+1,len(tupla1[1])): 
            if tupla1[1][i] >= tupla1[1][j]: 
                lista_conexiones.append(((tupla1[1][j],tupla1[1][i]),('pending',tupla1[0])))
            else:
                lista_conexiones.append(((tupla1[1][i],tupla1[1][j]),('pending',tupla1[0])))
    return lista_conexiones

def posible_triciclo(tupla2): 
    return ('exists' in tupla2[1] and len(tupla2[1])>= 2)


def triciclos(tupla2): 
    triciclo = []
    for relacion in tupla2[1]:
        if relacion != 'exists':
            triciclo.append((relacion[1],tupla2[0][0], tupla2[0][1]))
    return triciclo

def ejercicio_dos(sc,files):
    rdd = sc.parallelize([])
    for file in files:
        file_rdd = sc.textFile(file)
        rdd = rdd.union(file_rdd) 
    graph_clean = rdd.map(mapper).filter(lambda x: x != None).distinct()
    adyacents = graph_clean.groupByKey() 
    rdd_conexiones = adyacents.mapValues(list).flatMap(conexiones)  
    rdd_triciclos = rdd_conexiones.groupByKey().mapValues(list).filter(posible_triciclo).flatMap(triciclos) 
    print(rdd_triciclos.collect())
    return rdd_triciclos.collect()

if __name__ == "__main__":
    if len(sys.argv) <= 2:
        print(f"Uso: python3 {0} <file>")
    else:
        ejercicio_dos(sc, sys.argv[1:])
