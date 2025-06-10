# mock-stream-server.py
import socket
import time
import random
from datetime import datetime
from kafka import KafkaProducer

HOST = '0.0.0.0' #listen to all available networks 
PORT = 9998
#DURATION = 100

# create a TCP socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((HOST, PORT))
s.listen(1) 

print(f"Waiting for connection on {HOST}:{PORT}...")

#the main loop generating the data
while True:
    conn, addr = s.accept()
    print(f"Connected by {addr}")
    start_time = time.time()

    try:
        while True:
            #if time.time() - start_time > DURATION:
                #print("finished sending data. Closing connection")
                #break

            #create timestamp + value 
            timestamp = datetime.utcnow().isoformat()
            value = random.randint(1, 100)
            message = f"{timestamp},{value}\n"

            #send it
            conn.sendall(message.encode()) 

            #sleep randomly between 10ms and 50ms 
            time.sleep(random.uniform(0.01, 0.05))

            producer = KafkaProducer(bootstrap_servers=['kafka:9092'],
                                     value_serializer=lambda v: v.encode('utf-8'))
            producer.send('mock-data-stream', message)
            producer.flush()

    except (BrokenPipeError, ConnectionResetError): 
        print("Client disconnected. Waiting for new connection...")
        conn.close()
