import csv
import time
import math

# Función para verificar si un número es primo
def es_primo(n):
    if n <= 1:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    for i in range(3, int(math.sqrt(n)) + 1, 2):
        if n % i == 0:
            return False
    return True

# Función para leer el archivo CSV y contar los números primos
def contar_primos_en_csv(archivo_csv):
    primos_encontrados = 0
    # Leer el archivo CSV
    with open(archivo_csv, mode='r') as archivo:
        lector = csv.reader(archivo)
        for fila in lector:
            for valor in fila:
                try:
                    numero = int(valor)
                    if es_primo(numero):
                        primos_encontrados += 1
                except ValueError:
                    # Si no se puede convertir a número, lo ignoramos
                    continue
    return primos_encontrados

# Función principal
def main():
    archivo_csv = 'numeros_aleatorios.csv'  # Asegúrate de que el archivo esté en el mismo directorio

    # Iniciar el conteo del tiempo
    inicio = time.time()

    # Contar los números primos en el archivo CSV
    primos = contar_primos_en_csv(archivo_csv)

    # Finalizar el conteo del tiempo
    fin = time.time()

    # Mostrar el resultado
    tiempo_total = fin - inicio
    print(f"Cantidad de números primos encontrados: {primos}")
    print(f"Tiempo total de ejecución: {tiempo_total:.4f} segundos")

# Ejecutar el programa
if __name__ == "__main__":
    main()
