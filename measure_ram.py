# measure_ram.py
import psutil

def find_es_memory_usage():
    """Finds the memory usage of the main Elasticsearch java process."""
    es_process = None
    max_rss = 0

    # Look for all running java.exe processes
    for proc in psutil.process_iter(['pid', 'name', 'memory_info']):
        if 'java' in proc.info['name'].lower():
            # The main ES process will be the one using the most memory
            rss = proc.info['memory_info'].rss
            if rss > max_rss:
                max_rss = rss
                es_process = proc

    if es_process:
        rss_gb = max_rss / (1024 * 1024 * 1024)
        print("--- Elasticsearch Runtime Memory (Artifact C) ---")
        print(f"Process Name: {es_process.info['name']}")
        print(f"Process ID (PID): {es_process.info['pid']}")
        print(f"Runtime Memory (RSS): {rss_gb:.2f} GB")
    else:
        print("Could not find a running Elasticsearch (java) process.")

if __name__ == "__main__":
    find_es_memory_usage()