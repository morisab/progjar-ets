import socket
import json
import base64
import logging
import time
import os
import random
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

class FileClient:
    def __init__(self, server_address=('localhost', 6666)):
        self.server_address = server_address

    def send_command(self, command_str=""):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(self.server_address)
        try:
            sock.sendall(command_str.encode())
            data_received = ""
            while True:
                data = sock.recv(8192)
                if data:
                    data_received += data.decode()
                    if "\r\n\r\n" in data_received:
                        break
                else:
                    break
            hasil = json.loads(data_received)
            return hasil
        except Exception as e:
            logging.error(f"Error: {str(e)}")
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
            isifile = base64.b64decode(hasil['data_file'])
            return isifile
        return None

    def remote_upload(self, filename="", file_data=None):
        if file_data is None:
            try:
                with open(filename, 'rb') as f:
                    file_data = f.read()
            except Exception as e:
                return {"status": "ERROR", "data": str(e)}
        
        encoded_content = base64.b64encode(file_data).decode()
        command_str = f"UPLOAD {filename}\r\n{encoded_content}\r\n\r\n"
        return self.send_command(command_str)

def generate_test_file(filename, size_mb):
    size = size_mb * 1024 * 1024
    data = os.urandom(size)
    with open(filename, 'wb') as f:
        f.write(data)
    return filename

def worker_task(client, file_size_mb, worker_id):
    filename = f"testfile_{worker_id}_{file_size_mb}mb.dat"
    test_filename = f"source_{file_size_mb}mb.dat"
    results = {'worker_id': worker_id}
    
    try:
        # Generate source file if not exists
        if not os.path.exists(test_filename):
            generate_test_file(test_filename, file_size_mb)
        
        # PHASE 1: UPLOAD
        start_upload = time.time()
        with open(test_filename, 'rb') as f:
            file_data = f.read()
        
        upload_result = client.remote_upload(filename, file_data)
        upload_time = time.time() - start_upload
        
        results['upload'] = {
            'success': upload_result.get('status') == 'OK',
            'time': upload_time,
            'bytes': len(file_data),
            'server_response': upload_result
        }
        
        if not results['upload']['success']:
            results['download'] = {
                'success': False,
                'error': 'Upload failed, skipping download'
            }
            return results
            
        # PHASE 2: VERIFY FILE EXISTS
        list_result = client.remote_list()
        if filename not in list_result.get('data', []):
            results['download'] = {
                'success': False,
                'error': 'File not found on server after upload'
            }
            return results
        
        # PHASE 3: DOWNLOAD
        start_download = time.time()
        download_result = client.remote_get(filename)
        download_time = time.time() - start_download
        
        if download_result is None:
            results['download'] = {
                'success': False,
                'error': 'Server returned None'
            }
        else:
            results['download'] = {
                'success': True,
                'time': download_time,
                'bytes': len(download_result),
                'data_valid': download_result[:10] == file_data[:10]  # Verifikasi sebagian data
            }
            
    except Exception as e:
        results['error'] = str(e)
        logging.error(f"Worker {worker_id} failed: {str(e)}")

    return results

def run_full_test(server_address, file_size_mb, num_workers, use_process_pool=False):
    clients = [FileClient(server_address) for _ in range(num_workers)]
    executor_class = ProcessPoolExecutor if use_process_pool else ThreadPoolExecutor
    
    with executor_class(max_workers=num_workers) as executor:
        futures = [
            executor.submit(
                worker_task,
                clients[i % len(clients)],
                file_size_mb,
                i
            )
            for i in range(num_workers)
        ]
        
        all_results = []
        for future in as_completed(futures):
            all_results.append(future.result())
    
    # Calculate statistics
    successful_uploads = sum(1 for r in all_results if r.get('upload', {}).get('success', False))
    successful_downloads = sum(1 for r in all_results if r.get('download', {}).get('success', False))
    
    total_upload_bytes = sum(r['upload']['bytes'] for r in all_results if r.get('upload', {}).get('success', False))
    total_download_bytes = sum(r['download']['bytes'] for r in all_results if r.get('download', {}).get('success', False))
    
    max_upload_time = max(r['upload']['time'] for r in all_results if 'upload' in r)
    max_download_time = max(r['download']['time'] for r in all_results if 'download' in r and r['download']['success'])
    
    avg_upload_throughput = total_upload_bytes / max_upload_time if max_upload_time > 0 else 0
    avg_download_throughput = total_download_bytes / max_download_time if max_download_time > 0 else 0
    
    return {
        "file_size_mb": file_size_mb,
        "num_workers": num_workers,
        "pool_type": "Process" if use_process_pool else "Thread",
        "upload": {
            "successful": successful_uploads,
            "failed": num_workers - successful_uploads,
            "total_time": max_upload_time,
            "throughput": avg_upload_throughput,
            "total_bytes": total_upload_bytes
        },
        "download": {
            "successful": successful_downloads,
            "failed": num_workers - successful_downloads,
            "total_time": max_download_time,
            "throughput": avg_download_throughput,
            "total_bytes": total_download_bytes
        }
    }

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='File Server Stress Test Client')
    parser.add_argument('--server', default='localhost:6666', help='Server address (host:port)')
    parser.add_argument('--file-size', type=int, choices=[10, 50, 100], required=True, help='File size in MB')
    parser.add_argument('--workers', type=int, choices=[1, 5, 50], required=True)
    parser.add_argument('--use-process-pool', action='store_true', help='Use process pool instead of thread pool')

    args = parser.parse_args()
    
    host, port = args.server.split(':')
    server_address = (host, int(port))
    
    result = run_full_test(
        server_address,
        args.file_size,
        args.workers,
        args.use_process_pool
    )
    
    print("\n=== Stress Test Results ===")
    print(f"File Size: {result['file_size_mb']} MB")
    print(f"Workers: {result['num_workers']}")
    print(f"Pool Type: {result['pool_type']}")
    
    print("\n=== Upload Statistics ===")
    print(f"Successful: {result['upload']['successful']}")
    print(f"Failed: {result['upload']['failed']}")
    print(f"Total Time: {result['upload']['total_time']:.2f} seconds")
    print(f"Throughput: {result['upload']['throughput']/1024/1024:.2f} MB/s")
    print(f"Total Bytes: {result['upload']['total_bytes']/1024/1024:.2f} MB")
    
    print("\n=== Download Statistics ===")
    print(f"Successful: {result['download']['successful']}")
    print(f"Failed: {result['download']['failed']}")
    print(f"Total Time: {result['download']['total_time']:.2f} seconds")
    print(f"Throughput: {result['download']['throughput']/1024/1024:.2f} MB/s")
    print(f"Total Bytes: {result['download']['total_bytes']/1024/1024:.2f} MB")