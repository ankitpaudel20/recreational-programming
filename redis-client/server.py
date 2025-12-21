import socket
import threading

def handle_conn(conn,addr):
    print(f"{addr=} | {type(conn)=}")
    while True:
        data=conn.recv(1024)
        if not data:
            return
        print(f"received {data=}")

s= socket.socket(socket.AF_INET, socket.SOCK_STREAM)

s.bind(("localhost",9000))

s.listen()

active_conn_threads=[]
while True:
    conn,addr=s.accept()
    print(f"received connection from {addr=} spinning a new thread to handle data transfer")
    handling_thread=threading.Thread(target=handle_conn, args=(conn,addr))
    handling_thread.start()
    active_conn_threads.append(handling_thread)
