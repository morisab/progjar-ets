from socket import *
import socket
import logging
import sys
from multiprocessing import Pool
from file_protocol import FileProtocol

class ProcessTheClient:
    def __init__(self):
        self.fp = FileProtocol()

    def __call__(self, conn_addr):
        connection, address = conn_addr
        d = ''
        while True:
            data = connection.recv(8192)
            if data:
                d += data.decode()
                if "\r\n\r\n" in d:
                    hasil = self.fp.proses_string(d.strip())
                    hasil = hasil + "\r\n\r\n"
                    connection.sendall(hasil.encode())
                    break
            else:
                break
        connection.close()
        return True

class ServerProcessPool:
    def __init__(self, ipaddress='0.0.0.0', port=8889, max_workers=5):
        self.ipinfo = (ipaddress, port)
        self.max_workers = max_workers
        self.my_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.my_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.process_pool = Pool(processes=self.max_workers)

    def run(self):
        logging.warning(f"server berjalan di ip address {self.ipinfo} dengan process pool (max_workers={self.max_workers})")
        self.my_socket.bind(self.ipinfo)
        self.my_socket.listen(5)
        client_handler = ProcessTheClient()
        while True:
            connection, client_address = self.my_socket.accept()
            logging.warning(f"connection from {client_address}")
            self.process_pool.apply_async(client_handler, args=((connection, client_address),))

def main():
    max_workers = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    svr = ServerProcessPool(ipaddress='0.0.0.0', port=6666, max_workers=max_workers)
    svr.run()

if __name__ == "__main__":
    main()