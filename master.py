import grpc
from concurrent import futures
import mapreduce_pb2
import mapreduce_pb2_grpc
import time

WORKERS = ["localhost:50052", "localhost:50053", "localhost:50054"]  # Example worker nodes


class MapReduceMaster(mapreduce_pb2_grpc.MapReduceServiceServicer):
    def SubmitJob(self, request, context):
        job_id = request.job_id
        input_data = request.input_data
        map_function = request.map_function
        reduce_function = request.reduce_function

        # Print job details when received - make sure these are displayed
        print("\n" + "="*50)
        print(f"Job Submitted: {job_id}")
        print(f"Input Data: {str(input_data)[:100]}...")
        print(f"Map Function: {str(map_function)[:100]}...")
        print(f"Reduce Function: {str(reduce_function)[:100]}...")
        print("="*50 + "\n")

        # For debugging - flush stdout to ensure prints are shown immediately
        import sys
        sys.stdout.flush()

        # Assign Map tasks to workers
        print(f"Starting Map phase for job {job_id}...")
        intermediate_results = []
        for i, chunk in enumerate(input_data):
            worker_index = i % len(WORKERS)
            worker_address = WORKERS[worker_index]
            print(f"Assigning map task to worker at {worker_address}")
            
            try:
                worker_stub = self.get_worker_stub(worker_index)
                response = worker_stub.ProcessMap(
                    mapreduce_pb2.MapTask(job_id=job_id, data_chunk=chunk, map_function=map_function)
                )
                print(f"Received {len(response.intermediate_data)} intermediate results from worker {worker_index}")
                intermediate_results.extend(response.intermediate_data)
            except Exception as e:
                print(f"Error during map task: {e}")
                return mapreduce_pb2.JobResponse(job_id=job_id, status=f"Error: {str(e)}")

        print(f"Map phase complete. Total intermediate results: {len(intermediate_results)}")

        # Assign Reduce task
        print(f"Starting Reduce phase for job {job_id}...")
        try:
            worker_stub = self.get_worker_stub(0)  # Pick a worker for Reduce
            print(f"Assigning reduce task to worker at {WORKERS[0]}")
            reduce_response = worker_stub.ProcessReduce(
                mapreduce_pb2.ReduceTask(
                    job_id=job_id, intermediate_data=intermediate_results, reduce_function=reduce_function
                )
            )
            print(f"Reduce phase complete for job {job_id}")
            print(f"Job Result: {reduce_response.output[:100]}...")
        except Exception as e:
            print(f"Error during reduce task: {e}")
            return mapreduce_pb2.JobResponse(job_id=job_id, status=f"Error: {str(e)}")

        return mapreduce_pb2.JobResponse(job_id=job_id, status=reduce_response.output)

    def get_worker_stub(self, index):
        worker_address = WORKERS[index % len(WORKERS)]
        channel = grpc.insecure_channel(worker_address)
        return mapreduce_pb2_grpc.MapReduceServiceStub(channel)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_MapReduceServiceServicer_to_server(MapReduceMaster(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("="*50)
    print("Master Server started on port 50051")
    print("Ready to receive MapReduce jobs")
    print("="*50)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()