import grpc
import mapreduce_pb2
import mapreduce_pb2_grpc

channel = grpc.insecure_channel("localhost:50051")
stub = mapreduce_pb2_grpc.MapReduceServiceStub(channel)

# Custom user-defined Map function
map_function_code = """
def map_function(data):
    words = data.split()
    return [word.lower() for word in words]
"""

# Custom user-defined Reduce function
reduce_function_code = """
def reduce_function(intermediate_data):
    word_count = {}
    for word in intermediate_data:
        word_count[word] = word_count.get(word, 0) + 1
    return str(word_count)
"""

# Sample input data
# input_data = ["Hello world", "gRPC makes MapReduce easy"]

with open("input_large.txt", "r") as f:
    input_data = f.read().split(sep=" ")

# Send job request to the master
response = stub.SubmitJob(
    mapreduce_pb2.JobRequest(
        job_id="job1", input_data=input_data, map_function=map_function_code, reduce_function=reduce_function_code
    )
)

print("Final Output:", response.status)
