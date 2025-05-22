from socket import *
import socket
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
from file_protocol import FileProtocol

class ProcessTheClient:
    def __init__(self, connection, address):
        self.connection = connection
        self.address = address
        self.fp = FileProtocol()

    def run(self):
        d = ''
        while True:
            data = self.connection.recv(8192)
            if data:
                d += data.decode()
                if "\r\n\r\n" in d:
                    hasil = self.fp.proses_string(d.strip())
                    hasil = hasil + "\r\n\r\n"
                    self.connection.sendall(hasil.encode())
                    break
            else:
                break
        self.connection.close()
        return True

class ServerThreadPool:
    def __init__(self, ipaddress='0.0.0.0', port=8889, max_workers=5):
        self.ipinfo = (ipaddress, port)
        self.max_workers = max_workers
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.thread_pool = ThreadPoolExecutor(max_workers=self.max_workers)

    def run(self):
        logging.warning(f"server berjalan di ip address {self.ipinfo} dengan thread pool (max_workers={self.max_workers})")
        self.my_socket.bind(self.ipinfo)
        self.my_socket.listen(5)
        while True:
            self.connection, self.client_address = self.my_socket.accept()
            logging.warning(f"connection from {self.client_address}")
            
            client_handler = ProcessTheClient(self.connection, self.client_address)
            self.thread_pool.submit(client_handler.run)

def main():
    max_workers = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    svr = ServerThreadPool(ipaddress='0.0.0.0', port=6666, max_workers=max_workers)
    svr.run()

if __name__ == "__main__":
    main()