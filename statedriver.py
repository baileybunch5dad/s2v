


from concurrent import futures
import logging
import os
import grpc
import handshake_pb2
import handshake_pb2_grpc
import pandas as pd
import numpy as np
import multiprocessing
import threading
import time
import sys
from DynamicDist import DynamicDist
import tail_risk
import pyarrow as pa
import io
from datetime import datetime
import traceback
import socket
import psycopg2

import io

# Sample data: list of tuples (name, age)
data = [
    ("Alice", 25, 19.6),
    ("Bob", 30, 17.8),
    ("Charlie", 22, 99.3),
]

# Convert data to CSV format in memory
csv_buffer = io.StringIO()
for row in data:
    csv_string = ",".join(map(str, row))
    csv_buffer.write(csv_string + "\n")
csv_buffer.seek(0)  # Move cursor to start
print(csv_buffer.getvalue())


channelName = "localhost:46693"
channel = grpc.insecure_channel(channelName)
stub = handshake_pb2_grpc.HandShakeStub(channel)

print("Hello")
request = handshake_pb2.HelloRequest(name='freddy')
print(request)
response = stub.Hello(request)
print(response)
print(response.reply)

print("GetState")
request = handshake_pb2.EmptyRequest()
print(request)
response = stub.GetState(request)
print(f"{response=}")
print(f"Response to get status was {handshake_pb2.DistStatus.Name(response.workerstatus)}")
print(f"or simple {response.workerstatus=}")

print("SetState")
request = handshake_pb2.StateMessage(workerstatus=handshake_pb2.COLLECTING_DATA)
print(f"{request=}")
stub.SetState(request)

print("GetState")
request = handshake_pb2.EmptyRequest()
print(request)
response = stub.GetState(request)
print(f"{response=}")
print(f"Response to get status was {handshake_pb2.DistStatus.Name(response.workerstatus)}")
print(f"or simple {response.workerstatus=}")
