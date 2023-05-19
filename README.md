# Triciclos_PRPA

Estructuras auxiliares:

    Cuando se nombra tupla1, esta es un par cuya primera componente presenta uno y la segunda su adyacencia.
    Cuando se nombra tupla2, esta es un par cuya primera componente presenta dos nodos y la segunda una lista con las conexiones entre ellos.

Funciones:

    mapper: limpia el grafo, es decir, ordena los pares de nodos para que siempre sea (menor, mayor) y no considera las aristas que vayan de un nodo en si mismo.
    conexiones: dada una tupla 1 devuelve las correspondientes tuplas2.
    posible_triciclo: con esta funcion se "descartan" los casos en los que no haya relación directa o haya menos de dos conexiones.
    triciclo: se aplica despues de posible_triciclo y devuelve los triciclos.
    ejercicio_uno: la función que hace lo que pedía el enunciado.
    ejercicio_dos: la función que hace lo que pedía el enunciado, juntando todos los archivos en un RDD.
    ejercicio_tres: la función que hace lo que pedía el enunciado, juntando todos los archivos en un RDD marcando de qué archivo viene cada arista.
