"""

"""
from __future__ import print_function
import json
import os
import datetime
import logging

__author__ = "SasG"
__date__ = "17/05/17"
__version__ = "0.0.1"
__reviewer__ = ""

#re-direct stdout to log file
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger()
print = logger.info

class Stream2Log():
    """
    Class to write timing parameters from EIGER ZMQ stream image frames to log file
    """
    def __init__(self, basename="realtime", path=".", ftype=".log", verbose=False):

        self.basename = basename
        self.ftype = ftype
        self.path = path

        if not os.path.isdir(self.path):
            raise IOError("[ERR] path %s does not exist" %self.path)

        fname = os.path.join(path,basename+ftype)
        self._verbose = verbose

        print("[OK] write timing information from stream to %s" %fname)

        logger.addHandler(logging.FileHandler(fname, 'a'))

    def decodeFrames(self, frames):
        """
        decode and proces EIGER ZMQ stream frames
        """
        try:
            header = json.loads(frames[0].bytes)
            if header["htype"].startswith("dheader-"):
                self._decodeHeader(frames)
            elif header["htype"].startswith("dimage-"):
                self._decodeImage(frames)
            elif header["htype"].startswith("dseries_end"):
                self._decodeEndOfSeries(frames)
            else:
                raise IOError("[ERR] not an EIGER ZMQ message")
        except Exception as e:
            print(e)
            return False

        return True

    def _decodeImage(self, frames):
        """
        decode time parameters from ZMQ image frames
        """
        info = json.loads(frames[0].bytes)
        config = json.loads(frames[3].bytes)
        print("[%s] image info:" % self._getTimeStamp())
        print("\t %s" %info)
        print("\t %s" %config)

        return config

    def _decodeEndOfSeries(self, frames):
        series = json.loads(frames[0].bytes)["series"]
        print("[%s] end of series %s" %(self._getTimeStamp(),series))
        return True

    def _decodeHeader(self, frames):
        """
        decode and process ZMQ header frames
        """
        header = json.loads(frames[0].bytes)
        if header["header_detail"]:
            print("[%s] start series %s" %(self._getTimeStamp(),header["series"]))
        if self._verbose and header["header_detail"] is not "none":
            print("[OK] detector config")
            for key, value in json.loads(frames[1].bytes).iteritems():
                print("\t %s: %s" %(key, value))
        if self._verbose and header["header_detail"] == "all":
            if json.loads(frames[2].bytes)["htype"].startswith("dflatfield"):
                print("[*] received flatfield")
            if json.loads(frames[4].bytes)["htype"].startswith("dpixelmask"):
                print("[*] received pixel mask")
            if json.loads(frames[6].bytes)["htype"].startswith("dcountrate"):
                print("[*] received LUT")
        if len(frames) == 9:
            if self._verbose:
                print("[*] received appendix: %s" %json.loads(frames[8].bytes))

    def _getTimeStamp(self):
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
