#!/usr/bin/env python
"""
Test if EIGER ZMQ frames can be received. Output time and length
of the received frames to stdout.

usage: testStream.py [-h] -i IP [-p PORT]

Receive EIGER ZMQ frames and print receive time

optional arguments:
  -h, --help            show this help message and exit
  -i IP, --ip IP        EIGER host ip
  -p PORT, --port PORT  EIGER host port

"""

__author__ = "SasG"
__date__ = "17/05/19"
__version__ = "1.0"
__reviewer__ = ""

import zmq
import argparse
import datetime
import requests
import json

print("Current libzmq version is %s" % zmq.zmq_version())
print("Current  pyzmq version is %s" % zmq.__version__)

class ZMQStream():
    def __init__(self, host, apiPort=80, streamPort=9999):
        """
        create stream listener object
        """
        self._host = host
        self._streamPort = streamPort
        self._apiPort = apiPort

        self.initEigerStream()

        self.connect()

    def connect(self):
        """
        open ZMQ pull socket
        return receiver object
        """

        context = zmq.Context()
        receiver = context.socket(zmq.PULL)
        receiver.connect("tcp://{0}:{1}".format(self._host,self._streamPort))

        self._receiver = receiver
        print "[{2}] initialized stream receiver for host tcp://{0}:{1}".format(self._host,self._streamPort,getTime())
        return self._receiver

    def receive(self):
        """
        receive and return zmq frames if available
        """
        if self._receiver.poll(100): # check if message available
            frames = self._receiver.recv_multipart(copy = False)
            t = getTime()
            print "[%s] received zmq frames with length %d" %(t,len(frames))

            return frames

    def initEigerStream(self):
        """
        activate stream interface

        """
        answer = requests.put("http://%s:%s/stream/api/1.5.0/command/initialize" %(self._host,self._apiPort), data=None)
        if answer.status_code != 200:
            raise IOError("could not initialize stream on host %s:%s" %(self._host,self._apiPort))

        baseurl = "http://%s:%s/stream/api/1.5.0/config/" %(self._host,self._apiPort)
        headers = {"Content-Type": "application/json"}

        settings = {"mode":"enabled",
                    "header_detail":"basic"}

        for key, value in settings.iteritems():
            data = json.dumps({"value":value})
            url = baseurl+key
            answer = requests.put(url, data= data, headers=headers)
            if answer.status_code != 200:
                raise IOError("could not set %s to %s (%s)" %(url,value,answer))
            print "[{0}] set {1} to {2}".format(getTime(),url,value)


    def close(self):
        """
        close and disable stream
        """
        print("[%s] close connection" %getTime())
        return self._receiver.close()

def getTime():
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

def parseArgs():
    """
    parse user input and return arguments
    """
    parser = argparse.ArgumentParser(description = "Receive EIGER ZMQ frames and print receive time")

    parser.add_argument("-i", "--ip", help="EIGER host ip", type=str, required=True)
    parser.add_argument("-a", "--apiPort", help="EIGER API port", type=int, default=80)
    parser.add_argument("-s", "--streamPort", help="EIGER zmq stream port", type=int, default=9999)
    return parser.parse_args()

if __name__ == "__main__":
    args = parseArgs()
    try:
        stream = ZMQStream(args.ip, args.apiPort, args.streamPort)
        print "[%s] stream listener ready" %getTime()
        while True:
            frames = stream.receive()

    except KeyboardInterrupt:
        stream.close()
