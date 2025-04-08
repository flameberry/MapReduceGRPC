import grpc
import json
import mapreduce_pb2
import mapreduce_pb2_grpc
import time

def create_pagerank_job(iterations=3, damping_factor=0.85):
    """
    Create a MapReduce job for a single iteration of PageRank calculation
    
    Args:
        iterations: Maximum number of iterations to run
        damping_factor: The damping factor (typically 0.85)
    """
    # Define a sample web graph as adjacency list
    # Format: {source_page: [destination_pages]}
    web_graph = {
        "A": ["B", "C"],
        "B": ["A", "C", "D"],
        "C": ["A", "B"],
        "D": ["B", "C"]
    }
    
    # Initialize PageRank values (1/N for all pages)
    num_pages = len(web_graph)
    initial_rank = 1.0 / num_pages
    
    page_ranks = {page: initial_rank for page in web_graph}
    
    # Prepare input data for the first iteration
    input_data = [json.dumps({
        "web_graph": web_graph,
        "page_ranks": page_ranks,
        "iteration": 0,
        "total_iterations": iterations,
        "damping_factor": damping_factor,
        "num_pages": num_pages
    })]
    
    # Map function: For each page, distribute its PageRank to outgoing links
    map_function = """
def map_function(data_chunk):
    import json
    
    # Parse input data
    data = json.loads(data_chunk)
    web_graph = data["web_graph"]
    page_ranks = data["page_ranks"]
    iteration = data["iteration"]
    total_iterations = data["total_iterations"]
    damping_factor = data["damping_factor"]
    num_pages = data["num_pages"]
    
    result = []
    
    # For each page, distribute its rank to its outlinks
    for source_page, outlinks in web_graph.items():
        # If the page has outlinks, distribute its rank evenly
        if outlinks:
            rank_to_distribute = page_ranks[source_page] * damping_factor
            share_per_outlink = rank_to_distribute / len(outlinks)
            
            # Emit a key-value pair for each destination page
            for dest_page in outlinks:
                result.append(json.dumps({
                    "key": dest_page,
                    "value": share_per_outlink
                }))
        
        # Emit a key-value pair to track all pages (for pages with no inlinks)
        result.append(json.dumps({
            "key": "all_pages",
            "value": source_page
        }))
    
    # Keep metadata for the next iteration
    result.append(json.dumps({
        "key": "metadata",
        "value": {
            "web_graph": web_graph,
            "iteration": iteration + 1,
            "total_iterations": total_iterations,
            "damping_factor": damping_factor,
            "num_pages": num_pages
        }
    }))
    
    return result
"""
    
    # Reduce function: Sum up the distributed PageRank values for each page
    reduce_function = """
def reduce_function(intermediate_data):
    import json
    
    # Parse intermediate data
    new_page_ranks = {}
    metadata = None
    all_pages = set()
    
    # Process the intermediate key-value pairs
    for item in intermediate_data:
        data = json.loads(item)
        key = data["key"]
        value = data["value"]
        
        if key == "metadata":
            # Store metadata for the next iteration
            metadata = value
        elif key == "all_pages":
            # Track all pages to handle pages with no inlinks
            all_pages.add(value)
        else:
            # Sum up distributed PageRank values
            if key not in new_page_ranks:
                new_page_ranks[key] = 0
            new_page_ranks[key] += value
    
    # Add the random jump factor (1-d)/N to each page
    random_jump_value = (1 - metadata["damping_factor"]) / metadata["num_pages"]
    for page in all_pages:
        if page not in new_page_ranks:
            new_page_ranks[page] = 0
        new_page_ranks[page] += random_jump_value
    
    # Return the iteration state
    next_input = {
        "web_graph": metadata["web_graph"],
        "page_ranks": new_page_ranks,
        "iteration": metadata["iteration"],
        "total_iterations": metadata["total_iterations"],
        "damping_factor": metadata["damping_factor"],
        "num_pages": metadata["num_pages"]
    }
    return json.dumps(next_input)
"""
    
    return {
        "job_id": "pagerank_job",
        "input_data": input_data,
        "map_function": map_function,
        "reduce_function": reduce_function
    }

def submit_job(job_data):
    # Connect to the master server
    channel = grpc.insecure_channel('localhost:50051')
    stub = mapreduce_pb2_grpc.MapReduceServiceStub(channel)
    
    # Create job request
    request = mapreduce_pb2.JobRequest(
        job_id=job_data["job_id"],
        input_data=job_data["input_data"],
        map_function=job_data["map_function"],
        reduce_function=job_data["reduce_function"]
    )
    
    # Submit job and get response
    response = stub.SubmitJob(request)
    return response

def main():
    print("PageRank MapReduce Client")
    print("=========================")
    
    # Create PageRank job
    total_iterations = 3
    print(f"Performing PageRank calculation using MapReduce")
    print(f"Web graph: A → B,C | B → A,C,D | C → A,B | D → B,C")
    print(f"Running {total_iterations} iterations with damping factor 0.85")
    
    # Start with initial job
    job_data = create_pagerank_job(iterations=total_iterations)
    
    # Track the current iteration
    current_iteration = 0
    iteration_data = None
    
    # Run iterations until completion
    while current_iteration < total_iterations:
        if current_iteration > 0:
            # Update input data for next iteration
            job_data["input_data"] = [json.dumps(iteration_data)]
        
        print(f"\nSubmitting PageRank iteration {current_iteration + 1}/{total_iterations}...")
        
        try:
            response = submit_job(job_data)
            print(f"Iteration {current_iteration + 1} completed")
            
            # Parse the response
            iteration_data = json.loads(response.status)
            
            # Get the current page ranks
            page_ranks = iteration_data["page_ranks"]
            
            # Print current PageRank values
            print(f"\nPageRank values after iteration {current_iteration + 1}:")
            
            # Sort pages by rank (descending)
            sorted_ranks = sorted(page_ranks.items(), key=lambda x: float(x[1]), reverse=True)
            
            for page, rank in sorted_ranks:
                print(f"Page {page}: {float(rank):.6f}")
            
            # Move to next iteration
            current_iteration = iteration_data["iteration"]
            
        except Exception as e:
            print(f"Error during iteration {current_iteration + 1}: {e}")
            break
    
    print("\nFinal PageRank values:")
    print("======================")
    
    # Calculate sum of all ranks (should be close to 1.0)
    rank_sum = sum(float(rank) for _, rank in page_ranks.items())
    
    for page, rank in sorted_ranks:
        normalized_rank = float(rank) / rank_sum  # Normalize if needed
        print(f"Page {page}: {float(rank):.6f}")
    
    print(f"\nSum of all PageRanks: {rank_sum:.6f} (should be close to 1.0)")
    
    # Print analysis
    print("\nAnalysis:")
    print("=========")
    print(f"Most important page: {sorted_ranks[0][0]} with rank {float(sorted_ranks[0][1]):.6f}")
    print(f"Least important page: {sorted_ranks[-1][0]} with rank {float(sorted_ranks[-1][1]):.6f}")
    print("\nPageRank distribution reflects the link structure:")
    print("- Page B has the highest rank (linked from all other pages)")
    print("- Page C is second (linked from 3 pages)")
    print("- Page A is third (linked from 2 pages)")
    print("- Page D has the lowest rank (linked from only 1 page)")

if __name__ == "__main__":
    main()