import json
import subprocess
import csv
import time
from itertools import product

def start_server(server_type, workers):
    if server_type == 'thread':
        cmd = ['python3', 'file_server_threadpool.py', str(workers)]
    else:
        cmd = ['python3', 'file_server_processpool.py', str(workers)]
    return subprocess.Popen(cmd)

def run_client_test(operation, server_address, file_size, workers, use_process_pool):
    cmd = [
        'python3', 'stress_test_client.py',
        '--server', server_address,
        '--operation', operation,
        '--file-size', str(file_size),
        '--workers', str(workers)
    ]
    
    if use_process_pool:
        cmd.append('--use-process-pool')
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            return json.loads(result.stdout)
        return None
    except Exception as e:
        print(f"Client test failed: {str(e)}")
        return None

def main():
    test_matrix = [
        ('upload', 10, 1),
        ('upload', 10, 5),
        ('upload', 10, 50),
        ('upload', 50, 1),
        ('upload', 50, 5),
        ('upload', 50, 50),
        ('upload', 100, 1),
        ('upload', 100, 5),
        ('upload', 100, 50),
        ('download', 10, 1),
        ('download', 10, 5),
        ('download', 10, 50),
        ('download', 50, 1),
        ('download', 50, 5),
        ('download', 50, 50),
        ('download', 100, 1),
        ('download', 100, 5),
        ('download', 100, 50)
    ]
    
    server_types = ['thread', 'process']
    server_workers = [1, 5, 50]
    
    with open('stress_test_results.csv', 'w', newline='') as csvfile:
        fieldnames = [
            'test_id',
            'operation',
            'file_size_mb',
            'client_workers',
            'server_type',
            'server_workers',
            'total_time',
            'throughput_bytes_sec',
            'client_success',
            'client_failed',
            'server_status'
        ]
        
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        test_id = 1
        for operation, file_size, c_workers in test_matrix:
            for s_type, s_workers in product(server_types, server_workers):
                print(f"\nTest {test_id}: {operation} {file_size}MB with {c_workers} clients on {s_type} server ({s_workers} workers)")
                
                # Clean previous servers
                subprocess.run(['pkill', '-f', 'file_server'], stderr=subprocess.DEVNULL)
                time.sleep(1)
                
                server_proc = start_server(s_type, s_workers)
                time.sleep(2)  # Server warm-up
                
                try:
                    result = run_client_test(operation, 'localhost:6666', file_size, c_workers, False)
                    
                    if not result:
                        writer.writerow({
                            'test_id': test_id,
                            'operation': operation,
                            'file_size_mb': file_size,
                            'client_workers': c_workers,
                            'server_type': s_type,
                            'server_workers': s_workers,
                            'server_status': 'ERROR'
                        })
                        test_id += 1
                        continue
                        
                    writer.writerow({
                        'test_id': test_id,
                        'operation': operation,
                        'file_size_mb': file_size,
                        'client_workers': c_workers,
                        'server_type': s_type,
                        'server_workers': s_workers,
                        'total_time': result['total_time'],
                        'throughput_bytes_sec': result['throughput'],
                        'client_success': result['successful'],
                        'client_failed': result['failed'],
                        'server_status': 'OK' if server_proc.poll() is None else 'CRASHED'
                    })
                    
                    test_id += 1
                    
                finally:
                    server_proc.terminate()
                    server_proc.wait()
                    time.sleep(1)

if __name__ == '__main__':
    main()