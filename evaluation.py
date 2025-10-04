# evaluation.py
import time
import json
import asyncio
import numpy as np
from elasticsearch import AsyncElasticsearch, Elasticsearch
from src.es_indexer import ESIndexer

async def run_single_query(es_client, index_name, query_text):
    """Sends a single async query and returns its latency."""
    query_body = {"query": {"multi_match": {"query": query_text, "fields": ["content", "title"]}}}
    start_time = time.time()
    await es_client.search(index=index_name, query=query_body["query"])
    end_time = time.time()
    return (end_time - start_time) * 1000

async def run_parallel_evaluation():
    """Runs a parallel evaluation to measure throughput and latency."""
    print("--- Starting Parallel Evaluation for ESIndex-v1.0 ---")

    # --- Setup ---
    es_client = AsyncElasticsearch("http://localhost:9200")
    if not await es_client.ping():
        raise ConnectionError("Could not connect to Elasticsearch.")
    
    sync_es_client = Elasticsearch("http://localhost:9200")
    es_indexer = ESIndexer(sync_es_client)
    index_name = "esindex-v1.0"

    try:
        with open("queries.txt", "r", encoding="utf-8") as f:
            queries = [line.strip() for line in f.readlines() if line.strip()]
        print(f"Loaded {len(queries)} queries.")
    except FileNotFoundError:
        print("Error: queries.txt not found.")
        await es_client.close()
        return

    # --- Performance Test ---
    print("\n--- Running Performance Test in Parallel ---")
    total_start_time = time.time()
    
    tasks = [run_single_query(es_client, index_name, q) for q in queries]
    latencies = await asyncio.gather(*tasks)
    
    total_end_time = time.time()
    total_duration_s = total_end_time - total_start_time

    # --- THIS IS THE MISSING PART ---

    # --- Calculate Metrics ---
    # Artifact A: Latency
    p95 = np.percentile(latencies, 95)
    p99 = np.percentile(latencies, 99)
    avg_latency = np.mean(latencies)

    # Artifact B: Throughput
    throughput = len(queries) / total_duration_s

    # --- Memory Footprint (Artifact C) ---
    print("\n--- Measuring Memory Footprint ---")
    memory_footprint = es_indexer.get_memory_footprint(index_id=index_name)
    
    # --- Save Results ---
    results_summary = {
        "index_name": es_indexer.identifier_short,
        "test_type": "parallel",
        "total_queries": len(queries),
        "artifact_A_latency": {
            "average_ms": avg_latency,
            "p95_ms": p95,
            "p99_ms": p99
        },
        "artifact_B_throughput": {
            "queries_per_second": throughput
        },
        "artifact_C_memory": {
            "disk_footprint": memory_footprint
        }
    }
    
    output_filename = f"results_{es_indexer.identifier_short}_parallel.json"
    with open(output_filename, "w") as f:
        json.dump(results_summary, f, indent=4)
        
    # --- Print Final Summary ---
    print("\n--- Evaluation Complete ---")
    print(f"Average Latency (parallel): {avg_latency:.2f} ms")
    print(f"p95 Latency (parallel): {p95:.2f} ms")
    print(f"Throughput: {throughput:.2f} queries/sec")
    print(f"Disk Footprint: {memory_footprint}")
    print(f"\nAll results have been saved to: {output_filename}")

    # Close the async client session
    await es_client.close()


if __name__ == "__main__":
    asyncio.run(run_parallel_evaluation())