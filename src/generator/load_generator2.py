import pika
import time
import random
import math
import threading
import os

# --- Configuración ---
RABBITMQ_HOST = 'localhost'
QUEUE_NAME = 'task_queue'
DURATION = 300  # 5 minutos para capturar tendencias claras
NUM_THREADS = 50 
PEAK_START_TIME = 120 # El pico de carga empieza a los 2 minutos

running = True

def load_generator(thread_id):
    credentials = pika.PlainCredentials('user', 'password')
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
    
    try:
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
    except Exception as e:
        print(f"Hilo {thread_id} falló al conectar: {e}")
        return

    start_time = time.time()
    
    while running:
        elapsed = time.time() - start_time
        
        # --- Lógica de Poisson No Homogénea ---
        # Representamos la variación horaria descrita en la propuesta [cite: 25]
        if elapsed < PEAK_START_TIME:
            # Fase 1: Carga Baja (Valle)
            # Cada hilo espera entre 2 y 4 segundos
            wait_time = random.uniform(2.0, 4.0)
        else:
            # Fase 2: Carga Alta (Pico / "Rush Hour") [cite: 12]
            # Cada hilo inyecta mensajes agresivamente (0.1 a 0.5 segundos)
            wait_time = random.uniform(0.1, 0.5)
        
        time.sleep(wait_time)
        
        message = f"Msg de hilo {thread_id} - Time: {time.time()}"
        try:
            channel.basic_publish(
                exchange='',
                routing_key=QUEUE_NAME,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)
            )
        except Exception as e:
            print(f"Error en hilo {thread_id}: {e}")
            break

    connection.close()

def main():
    global running
    print(f"Iniciando generación de carga...")
    print(f"Fase valle: 0-{PEAK_START_TIME}s | Fase PICO: {PEAK_START_TIME}-{DURATION}s")
    
    threads = []
    for i in range(NUM_THREADS):
        t = threading.Thread(target=load_generator, args=(i,))
        t.start()
        threads.append(t)
        
    time.sleep(DURATION)
    running = False
    
    for t in threads:
        t.join()
        
    print("Simulación de carga completada.")

if __name__ == '__main__':
    main()