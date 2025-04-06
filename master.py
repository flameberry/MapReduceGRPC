import grpc
from concurrent import futures
import mapreduce_pb2
import mapreduce_pb2_grpc

WORKERS = ["localhost:50052", "localhost:50053", "localhost:50054"]  # Example worker nodes


class MapReduceMaster(mapreduce_pb2_grpc.MapReduceServiceServicer):
    def SubmitJob(self, request, context):
        job_id = request.job_id
        input_data = request.input_data
        map_function = request.map_function
        reduce_function = request.reduce_function

        # Assign Map tasks to workers
        intermediate_results = []
        for i, chunk in enumerate(input_data):
            worker_stub = self.get_worker_stub(i)
            response = worker_stub.ProcessMap(
                mapreduce_pb2.MapTask(job_id=job_id, data_chunk=chunk, map_function=map_function)
            )
            intermediate_results.extend(response.intermediate_data)

        # Assign Reduce task
        worker_stub = self.get_worker_stub(0)  # Pick a worker for Reduce
        reduce_response = worker_stub.ProcessReduce(
            mapreduce_pb2.ReduceTask(
                job_id=job_id, intermediate_data=intermediate_results, reduce_function=reduce_function
            )
        )

        return mapreduce_pb2.JobResponse(job_id=job_id, status=reduce_response.output)

    def get_worker_stub(self, index):
        channel = grpc.insecure_channel(WORKERS[index % len(WORKERS)])
        return mapreduce_pb2_grpc.MapReduceServiceStub(channel)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    mapreduce_pb2_grpc.add_MapReduceServiceServicer_to_server(MapReduceMaster(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("Master Server started on port 50051")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
