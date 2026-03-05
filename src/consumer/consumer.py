import pika
import time
import random
import os
import datetime

# Configuration
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'localhost')
RABBITMQ_USER = os.getenv('RABBITMQ_USER', 'user')
RABBITMQ_PASS = os.getenv('RABBITMQ_PASS', 'password')
QUEUE_NAME    = os.getenv('QUEUE_NAME', 'task_queue')

def connect():
    while True:
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=RABBITMQ_HOST, credentials=credentials)
            )
            return connection
        except pika.exceptions.AMQPConnectionError:
            print(f"Waiting for RabbitMQ at {RABBITMQ_HOST}...")
            time.sleep(5)

def callback(ch, method, properties, body):
    arrival_time = datetime.datetime.now().isoformat()
    print(f" [x] Received {body.decode()} at {arrival_time}")
    
    # Simulado (distribución exponencial)
    # Media = 0.5 segundos
    # processing_time = random.expovariate(1.0 / 0.5)

    # Real
    # μ = 2.0269 msg/s → media = 1/μ ≈ 0.4934 s
    processing_time = random.expovariate(2.0269)
    time.sleep(processing_time)
    
    completion_time = datetime.datetime.now().isoformat()
    print(f" [x] Done in {processing_time:.4f}s at {completion_time}")
    
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    connection = connect()
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    
    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)
    channel.start_consuming()

if __name__ == '__main__':
    main()
