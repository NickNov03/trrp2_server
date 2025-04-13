# recieves data from client, normalizes it, writes it to postgre 

from writer import writer as W
import socket
import yaml
import json
import pika

# SOCK

def recv_sock():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  # Create a socket object
    sock.bind((params['server_ip'], params['server_port']))  # Bind the host and port

    print('binded to', params['server_ip'], params['server_port'])

    sock.listen()
    print("Server is listening...")

    conn, address = sock.accept()  # Accept a connection
    print("Connection from: " + str(address))

    orders = []
    buffer = b''

    while True:
        header = conn.recv(1)
        msg_length = int.from_bytes(header, byteorder='big')
        data = conn.recv(msg_length)  # Receive data from the client

        if not data:
            break

        print(data)
        print(f"data type: {type(data)}, buffer type: {type(buffer)}")
        buffer += data
        while b'\n' in buffer:
            line, buffer = buffer.split(b'\n', 1)
            try:
                row = json.loads(line.decode('utf-8'))
                orders.append(row)
            except Exception as e:
                print(f"Ошибка обработки строки: {e}")

    conn.close()

    return orders

# MQ

def callback(ch, method, properties, body):
    data = json.loads(body.decode('utf-8'))
    print(" [x] Received:", data)
    
    process_data(ch, data)

    ch.basic_ack(delivery_tag=method.delivery_tag)

def process_data(ch, data):
    print(data)

    if data == 'ended!':
        ch.stop_consuming()
    else:
        orders.append(data)

    

def recv_mq():
    connection = pika.BlockingConnection(pika.ConnectionParameters(\
        host=params['mq_ip'], port=params['mq_port']))
    channel = connection.channel()
    
    channel.queue_declare(queue='data_queue', durable=True)
    
    channel.basic_consume(queue='data_queue', on_message_callback=callback)
    
    print(" [*] Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()

# MAIN

with open(r'config.yaml', 'r', encoding='utf8') as file:
# with open(r'D:\TRRP\lab2\server\config.yaml', 'r', encoding='utf8') as file:
    params = yaml.safe_load(file)

print(params)

if params['send_type'] == 'sock':
    orders = recv_sock()
elif params['send_type'] == 'mq':
    orders = []
    recv_mq()

print(orders)
w = W(params)
w.write(orders)
