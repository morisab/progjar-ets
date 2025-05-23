import socket
import json
import base64
import time
import os
import random
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

class FileClient:
    def __init__(self, server_address='localhost:6666'):
        # Parse server address properly
        if isinstance(server_address, str):
            host, port = server_address.split(':')
            self.server_address = (host, int(port))
        else:
            self.server_address = server_address

    def send_command(self, command_str=""):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(self.server_address)
            sock.sendall(command_str.encode())
            data_received = b""
            while True:
                data = sock.recv(8192)
                if data:
                    data_received += data
                    if b"\r\n\r\n" in data_received:
                        break
                else:
                    break
            hasil = json.loads(data_received.decode())
            return hasil
        except Exception as e:
            return {"status": "ERROR", "data": str(e)}
        finally:
            sock.close()

    def remote_list(self):
        command_str = "LIST\r\n\r\n"
        return self.send_command(command_str)

    def remote_get(self, filename=""):
        command_str = f"GET {filename}\r\n\r\n"
        hasil = self.send_command(command_str)
        if hasil['status'] == 'OK':
            return base64.b64decode(hasil['data_file'])
        return None

    def remote_upload(self, filename="", file_data=None):
        chunk_size = 10 * 1024 * 1024  # 10MB chunks
        encoded_chunks = []
        
        for i in range(0, len(file_data), chunk_size):
            chunk = file_data[i:i+chunk_size]
            encoded_chunks.append(base64.b64encode(chunk).decode())
        
        command_str = f"UPLOAD {filename}\r\n" + "".join(encoded_chunks) + "\r\n\r\n"
        return self.send_command(command_str)

def generate_test_file(filename, size_mb):
    chunk_size = 1024 * 1024  # 10MB chunks
    size = size_mb * chunk_size
    
    if os.path.exists(filename):
        with open(filename, 'rb') as f:
            existing_size = os.path.getsize(filename)
            if existing_size == size:
                return filename
    
    with open(filename, 'wb') as f:
        for _ in range(size_mb):
            f.write(os.urandom(chunk_size))
    return filename

def upload_worker(client, file_size_mb, worker_id):
    filename = f"testfile_{file_size_mb}mb.dat"
    test_filename = f"source_{file_size_mb}mb.dat"
    
    # Generate source file if not exists
    generate_test_file(test_filename, file_size_mb)
    
    with open(test_filename, 'rb') as f:
        file_data = f.read()
    
    start_time = time.time()
    result = client.remote_upload(filename, file_data)
    elapsed = time.time() - start_time
    list_result = client.remote_list()
    return {
        'success': result.get('status') == 'OK',
        'time': elapsed,
        'bytes': len(file_data),
        'worker_id': worker_id
    }

def download_worker(client, file_size_mb, worker_id):
    filename = f"testfile_{file_size_mb}mb.dat"
    start_time = time.time()
    result = client.remote_get(filename)
    elapsed = time.time() - start_time
    
    return {
        'success': result is not None,
        'time': elapsed,
        'bytes': len(result) if result else 0,
        'worker_id': worker_id
    }

def run_test(server_address, operation, file_size_mb, num_workers, use_process_pool=False):
    executor_class = ProcessPoolExecutor if use_process_pool else ThreadPoolExecutor
    worker_func = upload_worker if operation == 'upload' else download_worker
    
    # Create shared client for all workers
    client = FileClient(server_address)
    
    with executor_class(max_workers=num_workers) as executor:
        futures = [
            executor.submit(
                worker_func,
                client,
                file_size_mb,
                i
            )
            for i in range(num_workers)
        ]
        
        results = []
        for future in as_completed(futures):
            results.append(future.result())
    
    # Calculate statistics
    successful = sum(1 for r in results if r['success'])
    failed = num_workers - successful
    total_bytes = sum(r['bytes'] for r in results if r['success'])
    max_time = max(r['time'] for r in results)
    throughput = total_bytes / max_time if max_time > 0 else 0
    
    return {
        'operation': operation,
        'file_size_mb': file_size_mb,
        'client_workers': num_workers,
        'successful': successful,
        'failed': failed,
        'total_time': max_time,
        'throughput': throughput,
        'total_bytes': total_bytes
    }

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='File Server Stress Test Client')
    parser.add_argument('--server', default='localhost:6666', help='Server address (host:port)')
    parser.add_argument('--operation', choices=['upload', 'download'], required=True)
    parser.add_argument('--file-size', type=int, choices=[10, 50, 100], required=True)
    parser.add_argument('--workers', type=int, choices=[1, 5, 50], required=True)
    parser.add_argument('--use-process-pool', action='store_true')

    args = parser.parse_args()
    
    result = run_test(
        args.server,
        args.operation,
        args.file_size,
        args.workers,
        args.use_process_pool
    )
    
    print(json.dumps(result, indent=2))