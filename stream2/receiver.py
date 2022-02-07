import zmq
import logging, os, argparse
import multiprocessing
import time

logging.basicConfig(format='%(asctime)s | %(levelname)s: %(message)s', level=logging.INFO)

import decoding

def receiver(id, ip, port, outDir):
    verbosity=100
    
    context = zmq.Context()
    receiver = context.socket(zmq.PULL)
    receiver.connect(f'tcp://{ip}:{port}')
    logging.info(f'receiver {id} connected to tcp://{ip}:{port}, printing after {verbosity} received frames')
    
    msgs = 0
    while True:
        try:
            if receiver.poll(10):
                frames = receiver.recv(copy = False)
                msgs += 1
                if msgs%verbosity == 0:
                    logging.info(f'worker {id} received {msgs} frames')
                
                decoding.processMessage(frames, outDir)
        
        except Exception as e:
            logging.error(e)
            
             
def statusPoller(ip):
    import EigerClient
    c = EigerClient.EigerClient(ip)
    
    logging.info(f'enabling stream2 and setting header details to all')
    c.setStream2Config('enabled', True)
    c.setStream2Config('start_fields', 'all')
    
    
    
    while True:
        try:
            enabled = c.stream2Config('enabled')['value']
            status = c.detectorStatus('state')['value']
            temp = c.detectorStatus('temperature')['value']
            hv = c.detectorStatus('high_voltage/state')['value']
            
            logging.info(f'status: detector {status}\t temp: {temp:.2f} C\t HV: {hv}\t stream2 enabled: {enabled}')
            
        except Exception as e:
            logging.error(e)
        
        finally:
            time.sleep(2)
       


def parseArgs():
    parser = argparse.ArgumentParser(description = "receive and dump zmq messages to file")

    parser.add_argument("-i", "--ip", help="EIGER2 host ip", type=str, required=True)
    parser.add_argument("-p", "--port", help="EIGER2 stream2 port", type=int, default=31001)
    parser.add_argument("-n", "--nProcesses", help="number of receiver processes", type=int, default=4)
    parser.add_argument("-d", "--dir", help="/path/to/output/dir ", default = ".")

    args = parser.parse_args()

    return args

if __name__ == "__main__":
    args = parseArgs()
    
    os.makedirs(args.dir, exist_ok=True)
    
    multiprocessing.Process(target=statusPoller, args=(args.ip, )).start()
   
    for id in range(args.nProcesses):
        multiprocessing.Process(target=receiver, args=(id, args.ip, args.port, args.dir)).start()
