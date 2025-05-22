import subprocess
import csv
import time
from itertools import product

def start_server(server_type, workers):
    if server_type == 'thread':
        cmd = ['python3', 'file_server_threadpool.py', str(workers)]
    else:  # 'process'
        cmd = ['python3', 'file_server_processpool.py', str(workers)]
    return subprocess.Popen(cmd)

def run_client_test(server_address, file_size, workers, use_process_pool):
    cmd = [
        'python3', 'stress_test_client.py',
        '--server', server_address,
        '--file-size', str(file_size),
        '--workers', str(workers)
    ]
    
    if use_process_pool:
        cmd.append('--use-process-pool')
    
    try:
        # Increase timeout based on file size and workers
        # timeout = 30 + (file_size * workers)  # Base 30s + size*workers
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True,
            # timeout=timeout
        )
        return result.stdout
    # except subprocess.TimeoutExpired:
    #     print(f"Timeout ({timeout}s) reached for size {file_size}MB with {workers} workers")
    #     return None
    except Exception as e:
        print(f"Client test failed: {str(e)}")
        return None
    
def parse_result(output):
    if not output:
        return {}
        
    result = {}
    current_section = None
    
    for line in output.split('\n'):
        line = line.strip()
        if line.startswith('===') and line.endswith('==='):
            current_section = line.strip('= ').strip().lower().replace(' ', '_')
        elif ':' in line:
            key, value = map(str.strip, line.split(':', 1))
            key = key.lower().replace(' ', '_')
            
            # Key mapping for CSV compatibility
            key_mapping = {
                'total_time': 'time_s',
                'throughput': 'throughput_mb_s',
                'total_bytes': 'total_bytes_mb'
            }
            
            for old, new in key_mapping.items():
                if key.endswith(old):
                    key = key.replace(old, new)
            
            # Clean numeric values
            if any(unit in value for unit in ['MB/s', 'MB', 'seconds']):
                value = value.split()[0]
                
            if current_section and current_section != 'stress_test_results':
                result[f"{current_section}_{key}"] = value
            else:
                result[key] = value
    
    return result

def main():
    # Reduced test matrix for stability
    test_configs = [
        (10, 1),  # file_size, workers
        (10, 5),
        (10, 50),
        (50, 1),
        (50, 5),
        (50, 50),
        (100, 1),
        (100, 5),
        (100, 50)
    ]
    server_types = ['thread', 'process']
    server_workers = [1, 5, 50]
    
    with open('stress_test_results.csv', 'w', newline='') as csvfile:
        fieldnames = [
            'test_case',
            'file_size_mb',
            'client_workers',
            'server_type',
            'server_workers',
            'pool_type',
            'upload_successful',
            'upload_failed',
            'upload_time_s',
            'upload_throughput_mb_s', 
            'upload_total_bytes_mb',
            'download_successful',
            'download_failed',
            'download_time_s',
            'download_throughput_mb_s',
            'download_total_bytes_mb',
            'server_status',
            'notes'
        ]
        
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        test_num = 1
        for file_size, c_workers in test_configs:
            for s_type, s_workers in product(server_types, server_workers):
                print(f"\n=== Test Case {test_num} ===")
                print(f"File: {file_size}MB | Clients: {c_workers}")
                print(f"Server: {s_type} ({s_workers} workers)")
                
                # Clean previous servers
                subprocess.run(['pkill', '-f', 'file_server'], stderr=subprocess.DEVNULL)
                time.sleep(1)
                
                server_proc = start_server(s_type, s_workers)
                time.sleep(2)  # Server warm-up
                
                try:
                    start_time = time.time()
                    output = run_client_test('localhost:6666', file_size, c_workers, False)
                    elapsed = time.time() - start_time
                    
                    if not output:
                        print(f"Test failed after {elapsed:.1f}s")
                        writer.writerow({
                            'test_case': test_num,
                            'file_size_mb': file_size,
                            'client_workers': c_workers,
                            'server_type': s_type,
                            'server_workers': s_workers,
                            'notes': 'Test failed - no output'
                        })
                        test_num += 1
                        continue
                        
                    result = parse_result(output)
                    
                    # Prepare data row
                    row = {
                        'test_case': test_num,
                        'file_size_mb': file_size,
                        'client_workers': c_workers,
                        'server_type': s_type,
                        'server_workers': s_workers,
                        'pool_type': result.get('pool_type', 'thread'),
                        'upload_successful': result.get('upload_statistics_successful', ''),
                        'upload_failed': result.get('upload_statistics_failed', ''),
                        'upload_time_s': result.get('upload_statistics_time_s', ''),
                        'upload_throughput_mb_s': result.get('upload_statistics_throughput_mb_s', ''),
                        'upload_total_bytes_mb': result.get('upload_statistics_total_bytes_mb', ''),
                        'download_successful': result.get('download_statistics_successful', ''),
                        'download_failed': result.get('download_statistics_failed', ''),
                        'download_time_s': result.get('download_statistics_time_s', ''),
                        'download_throughput_mb_s': result.get('download_statistics_throughput_mb_s', ''),
                        'download_total_bytes_mb': result.get('download_statistics_total_bytes_mb', ''),
                        'server_status': 'Running' if server_proc.poll() is None else 'Crashed',
                        'notes': f'Completed in {elapsed:.1f}s'
                    }
                    
                    writer.writerow(row)
                    print(f"Test completed in {elapsed:.1f} seconds")
                    test_num += 1
                    
                except Exception as e:
                    print(f"Error during test case {test_num}: {str(e)}")
                    writer.writerow({
                        'test_case': test_num,
                        'notes': f'Error: {str(e)}'
                    })
                    test_num += 1
                finally:
                    server_proc.terminate()
                    server_proc.wait()
                    time.sleep(1)  # Cooldown between tests

if __name__ == '__main__':
    main()