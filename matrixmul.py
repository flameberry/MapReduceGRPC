import grpc
import json
import numpy as np
import mapreduce_pb2
import mapreduce_pb2_grpc

def create_matrix_multiplication_job():
    # Define two matrices for multiplication
    # Matrix A: 3x2
    matrix_a = [[1, 2], 
                [3, 4], 
                [5, 6]]
    
    # Matrix B: 2x3
    matrix_b = [[7, 8, 9], 
                [10, 11, 12]]
    
    # Expected result: 3x3 matrix
    # [27, 30, 33]
    # [61, 68, 75]
    # [95, 106, 117]
    
    # Prepare input data - we'll send both matrices as JSON
    input_data = [json.dumps({
        "matrix_a": matrix_a,
        "matrix_b": matrix_b,
        "a_rows": len(matrix_a),
        "a_cols": len(matrix_a[0]),
        "b_rows": len(matrix_b),
        "b_cols": len(matrix_b[0])
    })]
    
    # Map function: For each cell in the result matrix, generate key-value pairs
    # Key: (i,j) - position in result matrix
    # Value: partial product for that position
    map_function = """
def map_function(data_chunk):
    import json
    
    # Parse the input data
    data = json.loads(data_chunk)
    matrix_a = data["matrix_a"]
    matrix_b = data["matrix_b"]
    a_rows = data["a_rows"]
    a_cols = data["a_cols"]
    b_rows = data["b_rows"]
    b_cols = data["b_cols"]
    
    # Generate intermediate key-value pairs
    result = []
    
    for i in range(a_rows):
        for j in range(b_cols):
            for k in range(a_cols):
                # Key is the result cell position (i,j)
                # Value is the partial product A[i][k] * B[k][j]
                key_value = json.dumps({
                    "key": f"{i},{j}",
                    "value": matrix_a[i][k] * matrix_b[k][j]
                })
                result.append(key_value)
                
    return result
"""
    
    # Reduce function: Sum all partial products for each position
    reduce_function = """
def reduce_function(intermediate_data):
    import json
    
    # Dictionary to store the sum of partial products for each position
    result_cells = {}
    
    # Process intermediate data
    for item in intermediate_data:
        data = json.loads(item)
        key = data["key"]
        value = data["value"]
        
        if key not in result_cells:
            result_cells[key] = 0
        result_cells[key] += value
    
    # Determine the dimensions of the result matrix
    max_i = 0
    max_j = 0
    
    for key in result_cells.keys():
        i, j = map(int, key.split(','))
        max_i = max(max_i, i)
        max_j = max(max_j, j)
    
    # Create the result matrix
    result_matrix = []
    for i in range(max_i + 1):
        row = []
        for j in range(max_j + 1):
            key = f"{i},{j}"
            row.append(result_cells.get(key, 0))
        result_matrix.append(row)
    
    return json.dumps(result_matrix)
"""
    
    return {
        "job_id": "matrix_multiplication_job",
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
    print("Matrix Multiplication MapReduce Client")
    print("======================================")
    
    # Create job data
    job_data = create_matrix_multiplication_job()
    
    print(f"Submitting job with ID: {job_data['job_id']}")
    
    # Submit job
    try:
        response = submit_job(job_data)
        print(f"Job Status: {response.status}")
        
        # Parse and format the result
        result_matrix = json.loads(response.status)
        
        print("\nResult Matrix:")
        print("==============")
        for row in result_matrix:
            print(row)
        
        # Verify with NumPy
        matrix_a = np.array([[1, 2], [3, 4], [5, 6]])
        matrix_b = np.array([[7, 8, 9], [10, 11, 12]])
        expected_result = np.matmul(matrix_a, matrix_b)
        
        print("\nVerification with NumPy:")
        print("=======================")
        print(expected_result)
        
        # Check if our result matches NumPy's calculation
        calculation_matches = np.array_equal(np.array(result_matrix), expected_result)
        print(f"\nResults match: {calculation_matches}")
        
    except grpc.RpcError as e:
        print(f"RPC Error: {e}")
    
if __name__ == "__main__":
    main()