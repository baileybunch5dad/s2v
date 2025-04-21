# Copyright 2015 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""The Python implementation of the GRPC helloworld.Greeter server."""

from concurrent import futures
import logging
import os
import grpc
import handshake_pb2
import handshake_pb2_grpc

import multiprocessing

ports = [50051,50052]

class HandShakeServer(handshake_pb2_grpc.HandShake):
    def SendArray(self, request, context):
        print(f"Received array with {len(request.values)} values on OS process {os.getpid()}.")
        # Here, you could further process the array as needed
        return handshake_pb2.ArrayResponse(message="Array received successfully!")

    


def serve(port:int=50051):
    port = str(port)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    handshake_pb2_grpc.add_HandShakeServicer_to_server(HandShakeServer(), server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    processes = []
    for port in ports:
        process = multiprocessing.Process(target=serve, args=(port,), name="Server1")
        process.start()
        processes.append(process)
    for process in processes:
        process.join()
