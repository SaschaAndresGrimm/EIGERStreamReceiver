import zmq
import logging
import multiprocessing

IP = '192.168.30.26'
PORT = 31001
WORKERS = 4
XFRAMES = 1 # print every nth frame to stdout

logging.basicConfig(format='%(asctime)s | %(levelname)s: %(message)s', level=logging.NOTSET)

def worker(id, ip, port):
    context = zmq.Context()
    receiver = context.socket(zmq.PULL)
    receiver.connect(f'tcp://{ip}:{port}')
    logging.info(f'worker {id} connectet to tcp://{ip}:{port}, printing after {XFRAMES} received frames')
    
    
    msgs = 0
    while True:
        if receiver.poll(10):
            frames = receiver.recv_multipart(copy = False)
            msgs += 1
            if msgs%XFRAMES == 0:
            	logging.info(f'worker {id} received {msgs} messages')
       

if __name__ == "__main__":
    for id in range(WORKERS):
        multiprocessing.Process(target=worker, args=(id, IP, PORT)).start()
