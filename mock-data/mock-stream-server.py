# mock-stream-server.py
import socket
import time
import random

HOST = '0.0.0.0' #listen to all available networks 
PORT = 9999

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((HOST, PORT))
s.listen(1) #listen for 1 client 


print(f"Waiting for connection on {HOST}:{PORT}...")
#the main loop:
while True:
    conn, addr = s.accept()
    print(f"Connected by {addr}")
    try:
        while True:
            number = f"{random.randint(1, 100)}\n" #once connected, generate a random number between 1 and 100
            conn.sendall(number.encode()) #send it
            time.sleep(0.05) #wait 0.05 seconds
    except (BrokenPipeError, ConnectionResetError):
        print("Client disconnected. Waiting for new connection...")
        conn.close()
