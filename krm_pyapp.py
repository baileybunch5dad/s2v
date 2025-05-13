
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

# ports = [50051,50052,50053,50054]
# if len(sys.argv) > 1:
#     for i, arg in enumerate(sys.argv):
#         print(f"Argument {i}: {arg}")
#         if arg.startswith("--ports"):
#             portstr = arg[8:]
#             ports = list(map(int, portstr.split(',')))
# else:
#     print("No arguments provided.")

# print(f"Using ports {ports}")

class HandShakeServer(handshake_pb2_grpc.HandShakeServicer):

    # def __init__(self, server):
    #     self.server = server
        # self.logger = logging.getLogger('HandShakeService')

    def addStopEvent(self, stop_event):
        self._stop_event = stop_event
        
    def setOptions(self, port, id, server, credentials):
        self.port = port
        self.id = id
        self.server = server
        self.hists = {}
        self.printed = False
        self.credentials = credentials
        self._status = handshake_pb2.NOT_STARTED;

    def ProcessArrowStream(self, request, context):
        """Receives a serialized Arrow table, deserializes it, and processes it."""
        try:
            # Get the serialized bytes
            serialized_table = request.serialized_table
    
            # Deserialize the Arrow table
            reader = pa.ipc.open_stream(io.BytesIO(serialized_table))

            # pull evreything off the socket
            arrow_table = reader.read_all()

            # in a C call, convert the byte stream to a pandas dataframe
            df = arrow_table.to_pandas()

            if not self.printed:
                print(df)
                self.printed = True

            # also in C, gather the data in different groupby to stream them as arrays to the distribution finder 
            grps = df.groupby(by=['scenario'])

            if grps is None:
                print(f"ERROR: Received table with {arrow_table.num_rows} but no by group variable ")
                return handshake_pb2.ArrowTableResponse(
                    success=False,
                    message=f"Successfully received table with {arrow_table.num_rows} rows and {arrow_table.num_columns} columns")

            allkeys = []
            for grpid, grp in grps:
                # print(f"On {os.getpid()=} adding {grpid=}")
                # print(grp)
                key = str(grpid[0])
                if key not in self.hists.keys():
                    dd = DynamicDist()
                    self.hists[key] = dd
                else:
                    dd = self.hists[key]
                histdata = grp['par_bal'].to_numpy()
                # print(f"On {os.getpid()=} adding {histdata[:3]}... to hist for {key=} ")
                allkeys.append(key)
                dd.add_many(histdata)    

           
            return handshake_pb2.ArrowTableResponse(
                success=True,
                message=f"Successfully received table with {arrow_table.num_rows} rows and {arrow_table.num_columns} columns"
            )
        except Exception as e:
            stack_trace = traceback.format_exc()
            # Return error response
            error_message = f"{str(e)}\n{stack_trace}"
            context.set_details(error_message)
            context.set_code(grpc.StatusCode.INTERNAL)
            return handshake_pb2.ArrowTableResponse(
                success=False,
                message=f"Error processing Arrow table: {str(e)} {error_message}"
               )
        
    def ProcessData(self, request, context):
        """
        Process incoming data from the C++ client
        """
        # Prepare response
        response = handshake_pb2.StringResponse()
        
        # print(request)

        # Convert protobuf to pandas DataFrame
        df = self._protobuf_to_dataframe(request)

        # print(df)
        
        # Print the received data
        if not self.printed:
            print(f"Pqython:: PID {os.getpid()} receiving frames like ")
            print(df)
            sys.stdout.flush()
            self.printed = True

        grps = df.groupby(by=['scenario'])
        allkeys = []
        for grpid, grp in grps:
            # print(f"On {os.getpid()=} adding {grpid=}")
            # print(grp)
            key = str(grpid[0])
            if key not in self.hists.keys():
                dd = DynamicDist()
                self.hists[key] = dd
            else:
                dd = self.hists[key]
            histdata = grp['par_bal'].to_numpy()
            # print(f"On {os.getpid()=} adding {histdata[:3]}... to hist for {key=} ")
            allkeys.append(key)
            dd.add_many(histdata)    

        # print(f"On {os.getpid()=} updated distributions for {allkeys[:5]}... ")
        
        # Perform processing on the dataframe
        # For this example, we'll just report some basic statistics
        response.status = f"Successfully processed {len(df)} rows with {len(df.columns)} columns"
        
        return response
    
    def _protobuf_to_dataframe(self, table):
        """
        Convert a protobuf Table to a pandas DataFrame
        """
        data = {}
        
        # Process each column
        for column in table.columns:
            column_name = column.name
            column_type = column.type
            # column_values = []
            
            if column_type == 'double':
                column_values = list(column.double_array.v)
            elif column_type == 'int32':
                column_values = list(column.int32_array.v)
            elif column_type == 'int64':
                column_values = list(column.int64_array.v)
            elif column_type == 'string':
                column_values = list(column.string_array.v)
            elif column_type == 'date32[day]':
                clist = list(column.int32_array.v)
                days_since_epoch = np.array(clist)
                epoch = np.datetime64('1970-01-01')
                column_values = epoch + days_since_epoch.astype('timedelta64[D]')
            # Process values in the column
            # for value in column.values:
            # if column.HasField('double_array'):
            #     column_values = list(column.double_array.v)
            # elif column.HasField('string_array'):
            #     column_values = list(column.string_array.v)
            # elif column.HasField('int32_array'):
            #     column_values = list(column.int32_array.v)
            # elif column.HasField('int64_array'):
            #     column_values = list(column.int64_array.v)
            # elif column.HasField('long_array'):
            #     ll = list(column.long_array.v)
            #     lla = np.array(ll)
            #     llat = lla.astype('datetime64[s]')
            #     column_values = llat
            #     # column_values = [datetime.fromtimestamp(lv).strftime("%Y-%m-%d") for lv in ll]
            else:
                print(f"unknown column type {column_type}")
                column_values = []
            
            # Add the column to our data dictionary
            data[column_name] = column_values
        
        # Create and return DataFrame
        pdf = pd.DataFrame(data)
        return pdf

    def SendTable(self, request, context):
        dname: str = request.doublename
        dvals: np.array = np.array(request.doublevalues, dtype=np.float64)
        sname: str = request.stringname
        svals: np.array = np.array(request.stringvalues, dtype=str)
        dnametoks  = dname.split(' ')
        nds = len(dnametoks)
        nrows = len(dvals) / nds
        dc = {}
        for i in range(nds):
            colname = dnametoks[i]
            startidx = int(i*nrows)
            endidx = int((i+1)*nrows)
            coldata = dvals[startidx:endidx]
            dc[colname] = coldata
        snametoks  = sname.split(' ')
        nss = len(snametoks)
        nrows = len(svals) / nss
        for i in range(nds):
            colname = snametoks[i]
            startidx = int(i*nrows)
            endidx = int((i+1)*nrows)
            coldata = svals[startidx:endidx]
            dc[colname] = coldata
        
        df = pd.DataFrame(dc)
        if not self.printed:
            print(f"First frame recieved on {os.getpid()=} received\n{df}")
            self.printd = True
        grps = df.groupby(by=['scenario'])
        for grpid, grp in grps:
            # print(f"On {os.getpid()=} adding {grpid=}")
            # print(grp)
            key = str(grpid[0])
            if key not in self.hists.keys():
                dd = DynamicDist()
                self.hists[key] = dd
            else:
                dd = self.hists[key]
            histdata = grp['interest'].to_numpy()
            print(f"On {os.getpid()=} adding {histdata[:3]}... to hist for {key=} ")
            dd.add_many(histdata)
        return handshake_pb2.ArrayResponse(message="Array received successfully!")

    def GetState(self, request, context):
        try:
            return handshake_pb2.DistStatus(workerstatus=self._status)
        except Exception as e:
            stack_trace = traceback.format_exc()
            # Return error response
            error_message = f"{str(e)}\n{stack_trace}"
            context.set_details(error_message)
            context.set_code(grpc.StatusCode.INTERNAL)
            return handshake_pb2.DistStatus(workerstatus=self._status)
       
    
    def SetState(self, request, context):
        try:
            oldStatus = self._status
            self._status = request.DistStatus.workerstatus
            return handshake_pb2.DistStatus(workerstatus=oldStatus)
        except Exception as e:
            stack_trace = traceback.format_exc()
            # Return error response
            error_message = f"{str(e)}\n{stack_trace}"
            context.set_details(error_message)
            context.set_code(grpc.StatusCode.INTERNAL)
            return handshake_pb2.DistStatus(workerstatus=self._status)
    
    def GetHistograms(self, request, context):
        # allhists = []
        v = []
        for k,dd in self.hists.items():
            name=str(k)
            rbins, rhist, rnan_freq = dd.get_merge_data()
            # rbins, rhist = dd.histogram(n_bins=100) # Skip, get raw data instead
            double_array_np = np.array(rbins, dtype=np.float64)
            int_array_np = np.array(rhist, dtype=np.int32)
            # print(f"{name=} {int_array_np=} {double_array_np=}")
            d = handshake_pb2.SingleHistogram(name=name, int_array=int_array_np, double_array=double_array_np)
            v.append(d)
        # print(f"Return histograms and resetting hash from {os.getpid()}")
        self.hists = {}

        # for i in range(4):
        #     int_array_np = np.array(range(i+1,i+4))
        #     double_array_np = np.linspace(i+1.1, i+4.1, num=3)
        #     name = "Histogram#" + str(i)
        #     d = handshake_pb2.SingleHistogram(name=name, int_array=int_array_np, double_array=double_array_np)
        #     v.append(d)
            
        # for name,dd in self.hists.items():
        #     bins, hist = dd.histogram(n_bins=10)
        #     double_array = np.array(bins, dtype=np.float64)
        #     int_array = np.array(hist, dtype=np.int32)
        #     dstruct = handshake_pb2.SingleHistogram(name=name, int_array=int_array, double_array=double_array)
        #     allhists.append(dstruct)
        # response = handshake_pb2.MultipleHistograms(histograms=allhists)
        response = handshake_pb2.MultipleHistograms(histograms=v)
        return response


    # convert GRPC struct to 
    # { 
    # scenario1: {data=[], freqs=[]}, 
    # scenario2: {data=[], freqs=[]}
    # }
    def convert_to_native_dict(self, response):
        map_of_hists = {}
        for h in response.histograms:
            remote_key = h.name
            remote_data = np.array(list(h.double_array), dtype=np.float64)
            remote_freqs = np.array(list(h.int_array), dtype=np.int32)
            map_of_hists[remote_key] = { 'data': remote_data, 'freq': remote_freqs}
        return map_of_hists
    
    def merge(self, mapOfHists): 
        # print(f"Local keys: {self.hists.keys()}")
        # print(f"Remote keys: {mapOfHists.keys()}")
        localMerge = 0
        localAdd = 0
        for remote_key, subdict in mapOfHists.items():
            remote_data = subdict['data']
            remote_freqs = subdict['freq']
            if remote_key in self.hists.keys():
                # print(f"{remote_key=} present in both")
                localdd = self.hists[remote_key]
                localdd.merge(remote_data, remote_freqs)
                localMerge += 1
            else:
                print(f"{remote_key=} not present in local, adding")
                localdd = DynamicDist()
                localdd.merge(remote_data, remote_freqs)
                self.hists[remote_key] = localdd
                localAdd += 1
        print(f"map of distribution merges, {localAdd} keys added, {localMerge} distributions merged")
                            
    def AggregateLocal(self, request, context):
        print(f"On {os.getpid()=} AggregateLocal")
        try:
            ports: np.array = np.array(request.ports, dtype=int)
            if len(ports) > 1:
                for p in ports[1:]:
                # for p in ports:
                    channelName = "localhost:" + str(p)
                    print(f'Thread 0 aggregating its results with {channelName}')
                    if self.credentials is not None:
                        channel = grpc.secure_channel(channelName, self.credentials)
                    else:
                        channel = grpc.insecure_channel(channelName)
                    stub = handshake_pb2_grpc.HandShakeStub(channel)
                    print(f'Thread 0 aggregating its results with {channelName}')
                    request = handshake_pb2.GetHistogramsRequest(code=1)
                    otherThreadResults = stub.GetHistograms(request)
                    self.merge(self.convert_to_native_dict(otherThreadResults))
                    # tr = tail_risk.tail_risk(scenario_bins, scenario_freqs, dist="GPD")
            else:
                print('Thread 0 aggregating as sole thread')
            print(f"After aggregation have {len(self.hists.keys())} distribution keys")
            return handshake_pb2.AggregateLocalResponse(return_code=0)
        except Exception as e:
            stack_trace = traceback.format_exc()
            print(stack_trace)
            error_message = f"{str(e)}\n{stack_trace}"
            context.set_details(error_message)
            context.set_code(grpc.StatusCode.INTERNAL)
            return handshake_pb2.AggregateLocalResponse(
                return_code=1
                )

    def AggregateGlobal(self, request, context):
        print(f"On {os.getpid()=} AggregateGlobal")
        thishost = socket.gethostname()
        processedhosts = set()
        set.add(thishost)
        allhosts = getAllHosts()
        try:
            for channelName in allhosts:
                hname = channelName.split(":", 1)[0]
                if hname in processhosts:
                    print(f"Already processed {channelName}")
                else:
                    # channelName = server + ":" + str(ports[0])
                    print(f'Server 0 aggregating globally its results with {channelName}')
                    if self.credentials is not None:
                        channel = grpc.secure_channel(channelName, self.credentials)
                    else:
                        channel = grpc.insecure_channel(channelName)
                    stub = handshake_pb2_grpc.HandShakeStub(channel)
                    # channel = grpc.insecure_channel(channelName)
                    # stub = handshake_pb2_grpc.HandShakeStub(channel)
                    # print(f'Server 0 aggregating its results with {channelName}')
                    print(f'Thread 0 aggregating its results with {channelName}')
                    request = handshake_pb2.GetHistogramsRequest(code=1)
                    otherThreadResults = stub.GetHistograms(request)
                    self.merge(self.convert_to_native_dict(otherThreadResults))
                    request = handshake_pb2.StateMessage(worker_status=handshake_pb2.COMPLETED)
                    stub.SetState(request)
                    return handshake_pb2.AggregateGlobalResponse(return_code=0)
        except Exception as e:
            stack_trace = traceback.format_exc()
            print(stack_trace)
            error_message = f"{str(e)}\n{stack_trace}"
            context.set_details(error_message)
            context.set_code(grpc.StatusCode.INTERNAL)
            return handshake_pb2.AggregateLocalResponse(
                return_code=1
                )

    
    def SendDataFrame(self, request, context):
        print(request.columns)
        dct = {}
        for col in request.columns:
            # print(f'{col.name=}')
            if col.HasField("string_values"):
                # print("string columns")
                strvals: np.array = np.array(col.string_values.values, dtype=str)
                # print(strvals)
                dct[col.name] = strvals
            elif col.HasField('double_values'):
                # print("numeric columns")
                doublevals: np.array = np.array(col.double_values.values, dtype=np.float64)
                # print(doublevals)
                dct[col.name] = doublevals
            elif col.HasField('long_values'):
                # print("numeric columns")
                dtvals: np.array = np.array(col.datetime_values.values, dtype=np.int64)
                dtvals = dtvals.astype('datetime64[s]')
                # print(doublevals)
                dct[col.name] = dtvals
            else:
                print('Unknown column type')
        print(pd.DataFrame(dct))
        return handshake_pb2.StringResponse(str="It worked")

    def GetState(self, request, context):
        return handshake_pb2.StateMessage(workerstatus=self._status)
    
    def SetState(self, request, context):
        old_status = self._status
        self._status = handshake_pb2.StateMessage.workerstatus
        return handshake_pb2.StateMessage(workerstatus=old_status)
    
    def Hello(self, request, context):
        instr = str(request.name)
        print(f"Hello from TestMethod, received {instr}  {os.getpid()=} ")
        outstr = instr + " and back "
        return handshake_pb2.HelloResponse(reply=outstr)

    def GetResults(self, request, context):
        np.random.seed(0)
        dvals = np.random.uniform(low=100,high=200, size=3)
        response = handshake_pb2.StackedTable(doublename="Rocco"+str(self.id),
            doublevalues=dvals,
            stringname="Sameer"+str(self.id), 
            stringvalues=["IRBB","VAR","Net"])
        return response
    
    def Shutdown(self, request, context):
        print(f"Shutdown RPC called. Post message to terminate server.  {self.port=}")
        self._stop_event.set()
        # return my_service_pb2.ShutdownResponse()
        # shutdown_thread = threading.Thread(target=self._shutdown, args=())
        # shutdown_thread.daemon = True
        # shutdown_thread.start()
        # print(f"Daemon thread started, returning.  {self.port=}")
        return handshake_pb2.ShutdownResponse(status=f"Server shutting down {self.port=}")

    def _shutdown(self):
        time.sleep(1)  # Brief delay to allow response to be sent
        self.server.stop(grace=3)
        # sys.exit(0)

# def serve_memory(q, port: int=50051, id:int = 10):
#     import tracemalloc
#     tracemalloc.start()
#     port = str(port)
#     big_msg_options = [
#         ("grpc.max_send_message_length", 10 * 1024 * 1024),  # 10MB
#         ("grpc.max_receive_message_length", 1024 * 1024 * 1024)  # 1GB
#     ]
#     server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), options=big_msg_options)
#     hs = HandShakeServer()
#     hs.setOptions(port, id, server, tls)
#     handshake_pb2_grpc.add_HandShakeServicer_to_server(hs, server)
#     server.add_insecure_port("[::]:" + port)
#     server.start()
#     print(f"Server started, listening on {port} from {os.getpid()=}")
#     server.wait_for_termination()

def serve(q, id:int = 10, tls:bool = False):
    # print(f"SUBPROCESS Use secure channel {tls}")
    big_msg_options = [
        ("grpc.max_send_message_length", 10 * 1024 * 1024),  # 10MB
        ("grpc.max_receive_message_length", 1024 * 1024 * 1024)  # 1GB
    ]
    
    server = None
    hs = None
    newport = 0
    
    if tls:
            # Load SSL credentials
        with open("server.key", "rb") as f:
            private_key = f.read()
        with open("server.crt", "rb") as f:
            certificate = f.read()

        server_credentials = grpc.ssl_server_credentials([(private_key, certificate)])
        
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=5), options=big_msg_options)
        hs = HandShakeServer()
        handshake_pb2_grpc.add_HandShakeServicer_to_server(hs, server)

        newport = server.add_secure_port('[::]:0', server_credentials)
        client_credentials = grpc.ssl_channel_credentials(root_certificates=certificate)
        hs.setOptions(newport, id, server, client_credentials)

    else:
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=5), options=big_msg_options)
        hs = HandShakeServer()
        handshake_pb2_grpc.add_HandShakeServicer_to_server(hs, server)

        # Start the server on port 0 (dynamically assigned)
        newport = server.add_insecure_port('[::]:0')
        hs.setOptions(newport, id, server, None)
        
    stop_event = threading.Event()
    hs.addStopEvent(stop_event)
    server.start()
    q.put(newport)
    # print(f"Server started, listening on {newport} from PID={os.getpid()=}")
    sys.stdout.flush()
    
    # server.wait_for_termination()
    stop_event.wait()
    print("Shutting down server")
    server.stop(grace=5).wait()
    
    # print("Waiting on stop event")    

def getconnection() -> psycopg2.extensions.connection:
    if 'PGPASSWORD' not in os.environ:
        print('Need postgres connection environment variables PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSCHEMA')
        exit()
    PGHOST = os.getenv('PGHOST')
    PGPORT = os.getenv('PGPORT')
    PGDATABASE = os.getenv('PGDATABASE')
    PGUSER = os.getenv('PGUSER')
    PGPASSWORD = os.getenv('PGPASSWORD')
    PGSCHEMA = os.getenv('PGSCHEMA')
    conn = psycopg2.connect(dbname=PGDATABASE, user=PGUSER, password=PGPASSWORD, host=PGHOST, port=PGPORT,
                            options=f"-c search_path={PGSCHEMA}"    )
    return conn

def execsql(exstmt: str) -> None:
    print(exstmt)
    conn = getconnection()
    cur = conn.cursor()
    cur.execute(exstmt)
    cur.close()
    conn.commit()
    conn.close()
    
def createChannelTable():
    sqlstmt=f"CREATE TABLE IF NOT EXISTS rpcchannels (id SERIAL PRIMARY KEY, channel TEXT UNIQUE NOT NULL );"
    execsql(sqlstmt)
    
def addHostToTable(thishost):
    sqlstmt = f"INSERT INTO rpcchannels (channel) VALUES ('{thishost}') ON CONFLICT (channel) DO NOTHING;"
    execsql(sqlstmt)
    
def removeHostFromTable(thishost):
    sqlstmt = f"DELETE FROM rpcchannels WHERE channel='{thishost}';"
    execsql(sqlstmt)
    
def getAllHosts():
    sqlstmt = 'SELECT channel FROM rpcchannels;'
    conn = getconnection()
    print(sqlstmt)
    cursor = conn.cursor()
    cursor.execute(sqlstmt)
    channels = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return channels
    

# Profile the function and save output to a file
def serve_with_profiling(q, port:int=50051, id: int=0):
    import cProfile
    cProfile.runctx(f'serve({port},{id})', globals(), locals(), f'profile-{id}.out')

if __name__ == "__main__":
    # print("34 8764323 89734 987234")
    # for i in range(10):
    #     print("bananas are healthy")
    # logging.basicConfig( level=logging.INFO)
    with open('ports.txt','w') as portfl:
        portfl.write("")
    queue = multiprocessing.Queue()
    # queue = None
    processes = []
    nthreads = 4
    tls = False
    for argument in sys.argv[1:]:
        if argument.startswith("--nthreads="):
            nthreads = int(argument[11:])
        if argument.startswith("--tls") or argument.startswith("--secure"):
            tls = True
        if argument.startswith("--notls") or argument.startswith("--nosecure"):
            tls = False
    # print(f"Use secure channel {tls}")
    for idx in range(nthreads):
    # for idx, port in enumerate(ports):
        # process = multiprocessing.Process(target=serve_grpc_profiling, args=(port, idx), name="Server"+str(idx))
        # process = multiprocessing.Process(target=serve_with_profiling, args=(port, idx), name="Server"+str(idx))
        process = multiprocessing.Process(target=serve, args=(queue, idx, tls), name="Server"+str(idx))
        process.start()
        processes.append(process)
    time.sleep(1)
    ports = []
    while not queue.empty():
        ports.append(queue.get())
    queue.close()

    curhost = socket.gethostname()
    createChannelTable()
    with open('ports.txt','w') as portfl:
        for p in ports:
            portfl.write(str(p) + ' ')
            addHostToTable(curhost + ":" + str(p))
    for p in ports:
        print(str(p),end=' ')
    print()
    print(f"Python:: TLS encrytion {tls}")
    print(f"hosts={getAllHosts()}")
    sys.stdout.flush()

    # ports = []
    # with open('ports.txt','r') as fl:
    #     line = fl.readline()
    #     ports = [int(x) for x in line.split()]
    print(f"Python:: Started grpc servers on {ports} on host={socket.gethostname()} with fqdn={socket.getfqdn()}") 
    print("Python:: Waiting for servers to receive termination from krm_capp controller")
    sys.stdout.flush()
    for process in processes:
        process.join()
