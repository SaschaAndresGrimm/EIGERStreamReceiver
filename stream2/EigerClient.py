import requests, json, os
import numpy
from base64 import b64encode, b64decode
from threading import Thread

import logging, time
logging.basicConfig()
log = logging.getLogger(__name__)

def threaded(fn):
    """
    To use as decorator to make a function call threaded.
    """
    def wrapper(*args, **kwargs):
        thread = Thread(target=fn, args=args, kwargs=kwargs)
        thread.start()
        return thread
    return wrapper

class EigerClient:
    def __init__(self, ip, port=80, api = "1.8.0"):
        self._ip = ip
        self._port = port
        self._api = api
        
    def _get(self, url):
        resp = requests.get(url)
        return resp.json()
    
    def _put(self, url, value=None, headers={}):
        try:
            if value is not None:
                data = json.dumps({'value': value})
            else:
                data = json.dumps({})
            reply = requests.put(url, data=data, headers=headers)
            assert reply.status_code in range(200, 300), reply.reason
        except Exception as e:
            log.error(f'error {url}, {value}: {e}')
        
        time.sleep(0.1)
        try:
            return reply.json()
        except:
            return reply.content
        
    def _composeUrl(self, module, param, key):
        url = f'http://{self._ip}:{self._port}/{module}/api/{self._api}/{param}/{key}'
        log.debug(f'composed url: {url}')
        return url
  
    def _composeExperimentalUrl(self, module, param, key):
        url = f'http://{self._ip}:{self._port}/api/2-preview/{module}/{param}/{key}'
        log.debug(f'composed url: {url}')
        return url    
    
    def setDetectorConfig(self, key, value):
        url = self._composeUrl('detector','config',key)
        return self._put(url, value)

    def detectorConfig(self, key):
        url = self._composeUrl('detector','config', key)
        return self._get(url)

    def detectorStatus(self, key):
        url = self._composeUrl('detector','status', key)
        return self._get(url)

    def sendDetectorCommand(self, key):
        url = self._composeUrl('detector','command',key)
        return self._put(url)
    
    def setStreamConfig(self, key, value):
        url = self._composeUrl('stream','config',key)
        return self._put(url, value)

    def streamConfig(self, key):
        url = self._composeUrl('stream','config', key)
        return self._get(url)

    def streamStatus(self, key):
        url = self._composeUrl('stream','status', key)
        return self._get(url)

    def sendStreamCommand(self, key):
        url = self._composeUrl('stream','command',key)
        return self._put(url) 

    def setStream2Config(self, key, value):
        url = self._composeExperimentalUrl('stream','config',key)
        return self._put(url, value)

    def stream2Config(self, key):
        url = self._composeExperimentalUrl('stream','config', key)
        return self._get(url)

    def stream2Status(self, key):
        url = self._composeExperimentalUrl('stream','status', key)
        return self._get(url)

    def send2StreamCommand(self, key):
        url = self._composeExperimentalUrl('stream','command',key)
        return self._put(url) 

    def setMonitorConfig(self, key, value):
        url = self._composeUrl('monitor','config',key)
        return self._put(url, value)

    def monitorConfig(self, key):
        url = self._composeUrl('monitor','config', key)
        return self._get(url)

    def monitorStatus(self, key):
        url = self._composeUrl('monitor','status', key)
        return self._get(url)

    def sendMonitorCommand(self, key):
        url = self._composeUrl('monitor','command',key)
        return self._put(url)     
    
    def setFileWriterConfig(self, key, value):
        url = self._composeUrl('filewriter','config',key)
        return self._put(url, value)

    def fileWriterConfig(self, key):
        url = self._composeUrl('filewriter','config', key)
        return self._get(url)

    def fileWriterStatus(self, key):
        url = self._composeUrl('filewriter','status', key)
        return self._get(url)

    def sendFileWriterCommand(self, key):
        url = self._composeUrl('filewriter','command',key)
        return self._put(url)    

    def setSystemConfig(self, key, value):
        url = self._composeUrl('system','config',key)
        return self._put(url, value)

    def systemConfig(self, key):
        url = self._composeUrl('system','config', key)
        return self._get(url)

    def systemStatus(self, key):
        url = self._composeUrl('system','status', key)
        return self._get(url)

    def sendSystemCommand(self, key):
        url = self._composeUrl('system','command',key)
        return self._put(url)     

    def monitorImages(self, key='monitor'):
        url = f'http://{self._ip}:{self._port}/monitor/api/{self._api}/images/{key}'
        return requests.get(url, headers={'Content-type': 'application/tiff'}).content

    def getMask(self):
        darray = self.getDetectorConfig('pixel_mask')['value']
        data = numpy.frombuffer(b64decode(darray["data"]), dtype=numpy.dtype(str(darray["type"])))
        return data.reshape(darray["shape"])

    def setMask(self, ndarray):
        data = {"__darray__": (1, 0, 0),
                    "type": ndarray.dtype.str,
                    "shape": ndarray.shape,
                    "filters": ["base64"],
                    "data": b64encode(ndarray.data).decode('utf-8')}
        url = self._composeUrl('detector', 'config', 'pixel_mask')
        return self._put(url, value=data, headers={'Content-Type': 'application/json'})

    def getFlatfield(self):
        darray = self.getDetectorConfig('flatfield')['value']
        data = numpy.frombuffer(b64decode(darray["data"]), dtype=numpy.dtype(str(darray["type"])))
        return data.reshape(darray["shape"])

    def setFlatfield(self, ndarray):
        data = {"__darray__": (1, 0, 0),
                    "type": ndarray.dtype.str,
                    "shape": ndarray.shape,
                    "filters": ["base64"],
                    "data": b64encode(ndarray.data).decode('utf-8')}
        url = self._composeUrl('detector', 'config', 'flatfield')
        return self._put(url, value=data, headers={'Content-Type': 'application/json'})

    def fileWriterSave(self, fname, dir, delete=False):
        url = f'http://{self._ip}:{self._port}/data/{fname}'
        request = requests.get(url)
        with open(os.path.join(dir, fname), 'wb') as file:
            for chunk in request.iter_content(chunk_size=512 * 1024):
                if chunk:
                    file.write(chunk)
            file.close()
        if delete:
            requests.delete(url)
