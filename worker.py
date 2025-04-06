import grpc
from concurrent import futures
import mapreduce_pb2
import mapreduce_pb2_grpc
import argparse


class MapReduceWorker(mapreduce_pb2_grpc.MapReduceServiceServicer):
    def ProcessMap(self, request, context):
        map_function_code = request.map_function
        data_chunk = request.data_chunk

        # Execute the user-defined Map function
        local_scope = {}
        exec(map_function_code, {}, local_scope)
        map_function = local_scope.get("map_function")

        if map_function is None:
            return mapreduce_pb2.MapResult(job_id=request.job_id, intermediate_data=[])

        intermediate_data = map_function(data_chunk)

        return mapreduce_pb2.MapResult(job_id=request.job_id, intermediate_data=intermediate_data)

    def ProcessReduce(self, request, context):
        reduce_function_code = request.reduce_function
        intermediate_data = request.intermediate_data

        # Execute the user-defined Reduce function
        local_scope = {}
        exec(reduce_function_code, {}, local_scope)
        reduce_function = local_scope.get("reduce_function")

        if reduce_function is None:
            return mapreduce_pb2.ReduceResult(job_id=request.job_id, output="Error: No function found")

        output = reduce_function(intermediate_data)

        return mapreduce_pb2.ReduceResult(job_id=request.job_id, output=output)


def serve(args):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    mapreduce_pb2_grpc.add_MapReduceServiceServicer_to_server(MapReduceWorker(), server)
    server.add_insecure_port(f"[::]:{args.port}")
    server.start()
    print(f"Worker Server started on port {args.port}")
    server.wait_for_termination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="MapReduce-Worker")
    parser.add_argument("-p", "--port", type=int, default="50052", help="Port number to run the worker node on.")
    args = parser.parse_args()
    serve(args)
