import argparse
import zmq
import logging
import json, os, glob, time
import argparse

logging.basicConfig(format='%(asctime)s | %(levelname)s: %(message)s', level=logging.NOTSET)

def server(port, useStream2=False):
    context = zmq.Context(4)
    zmq_socket = context.socket(zmq.PUSH)
    zmq_socket.bind(f'tcp://127.0.0.1:{port}')
    logging.info(f'serving to tcp://127.0.0.1:{port}')

    for i, message in enumerate(frameGetter(useStream2)):
        logging.info(f'server sending message {i}')
        zmq_socket.send_multipart(message)

def frameGetter(stream2=False):
    if stream2:
        dir = 'stream2data'
    else:
        dir = 'streamData'
    files = sorted(glob.glob(os.path.join(dir,'*')))
    for file in files:
        with open(file, 'r') as f:
            yield f.read()
        

def parseArgs():
    parser = argparse.ArgumentParser(description='ZMQ test server')
    parser.add_argument('--port', '-z', type=int, default=99999, help="zmq tcp port")
    parser.add_argument('--useStream2', '-s2', action="store_true", help="use stream2 cbor frames")

    return parser.parse_args()


if __name__ == "__main__":
    args = parseArgs()
    t0 = time.time()
    server(args.port, args.useStream2)
    logging.info(f'finished in {time.time()-t0} seconds')
