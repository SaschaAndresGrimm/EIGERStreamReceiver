"""
FileWriter class is a dummy object which decodes the EIGER zmq
stream frames from header, images, and end of series meassages.
Inherite from this class and modify following functions in order
to create a FileWriter which actually saves files:
__decodeHeader__(self, frames)
__decodeImage__(self, frames)
__decodeEndOfSeries__(self, frames)
"""

import lz4.block, bitshuffle
import numpy as np
import json
import os
import struct

__author__ = "SasG"
__date__ = "16/11/22"
__version__ = "0.0.3"
__reviewer__ = ""

class FileWriter():
    """
    dummy class to decode zmq frames from EIGER ZMQ stream
    """
    def __init__(self, basename="eigerStream", path=".", ftype="", verbose=False, roi=False):

        self.basename = basename
        self.ftype = ftype
        self.path = path

        if not os.path.isdir(self.path):
            raise IOError("[ERR] path %s does not exist" %self.path)

        if roi: self.roi = ROI(*roi,verbose=verbose)
        else: self.roi = ROI(verbose=verbose)

        self._verbose = verbose
        if self._verbose:
            print "[OK] initialized %s FileWriter" %self.ftype

    def decodeFrames(self, frames):
        """
        decode and proces EIGER ZMQ stream frames
        """
        try:
            header = json.loads(frames[0].bytes)
        except Exception as e:
            print "[ERR] decoding header: %s" %str(e)
            return False
        if header["htype"].startswith("dheader-"):
            self.__decodeHeader__(frames)
        elif header["htype"].startswith("dimage-"):
            self.__decodeImage__(frames)
        elif header["htype"].startswith("dseries_end"):
            self.__decodeEndOfSeries__(frames)
        else:
            print "[ERR] not an EIGER ZMQ message"
            return False
        return True

    def __decodeImage__(self, frames):
        """
        decode ZMQ image frames
        """
        if self._verbose:
            print "[*] decode image"

        header = json.loads(frames[0].bytes) # header dict
        info = json.loads(frames[1].bytes) # info dict

        if info["encoding"] == "lz4<": #TODO: soft code flag
            data = self.readLZ4(frames[2], info["shape"], info["type"])
        elif "bs" in info["encoding"]:
            data = self.readBSLZ4(frames[2], info["shape"], info["type"])
        else:
            raise IOError("[ERR] encoding %s is not implemented" %info["encoding"])

        return data

    def __decodeEndOfSeries__(self, frames):
        if self._verbose:
            print "[OK] received end of series ", json.loads(frames[0].bytes)
            return True

    def __decodeHeader__(self, frames):
        """
        decode and process ZMQ header frames
        """
        if self._verbose:
            print "[*] decode header"
        header = json.loads(frames[0].bytes)
        if header["header_detail"]:
            if self._verbose:
                print header
        if header["header_detail"] is not "none":
            if self._verbose:
                print "detector config"
                for key, value in json.loads(frames[1].bytes).iteritems():
                    print key, value
        if header["header_detail"] == "all":
            if json.loads(frames[2].bytes)["htype"].startswith("dflatfield"):
                if self._verbose:
                    print "writing flatfield"
            if json.loads(frames[4].bytes)["htype"].startswith("dpixelmask"):
                if self._verbose:
                    print "writing pixel mask"
            if json.loads(frames[6].bytes)["htype"].startswith("dcountrate"):
                if self._verbose:
                    print "writing LUT"
        if len(frames) == 9:
            if self._verbose:
                print "[*] appendix: ", json.loads(frames[8].bytes)

    def readBSLZ4(self, frame, shape, dtype):
        """
        unpack bitshuffle-lz4 compressed frame and return np array image data
        frame: zmq data blob frame
        shape: image shape
        dtype: image data type
        """

        data = frame.bytes
        blob = np.fromstring(data[12:], dtype=np.uint8)
        dtype = np.dtype(dtype)
        # blocksize is big endian uint32 starting at byte 8, divided by element size
        blocksize = np.ndarray(shape=(), dtype=">u4", buffer=data[8:12])/dtype.itemsize
        imgData = bitshuffle.decompress_lz4(blob, shape[::-1], dtype, blocksize)
        if self._verbose:
            print "[OK] unpacked {0} bytes of bs-lz4 data".format(len(imgData))
        return imgData

    def readLZ4(self, frame, shape, dtype):
        """
        unpack lz4 compressed frame and return np array image data
        frame: zmq data blob frame
        shape: image shape
        dtype:image data type
        """

        dtype = np.dtype(dtype)
        dataSize = dtype.itemsize*shape[0]*shape[1] # bytes * image size


        imgData = lz4.block.decompress(struct.pack('<I', dataSize) + frame.bytes)
        #imgData = lz4.loads(struct.pack('<I', dataSize) + frame.bytes)
        if self._verbose:
            print "[OK] unpacked {0} bytes of lz4 data".format(len(imgData))

        return np.reshape(np.fromstring(imgData, dtype=dtype), shape[::-1])

class ROI():
    """
    NOT IMPLEMENTED YET
    software ROI
    class ROI returns region of interest of a numpy array
    """
    def __init__(self, start=[False,False], end=[False,False], verbose=False):

        self.xstart, self.ystart = start[0],start[1]
        self.xend, self.yend = end[0],end[1]
        self.active = all([self.xstart, self.ystart, self.xend, self.yend])
        self._verbose = verbose

        if self._verbose:
            if self.active:
                print "ROI active, start: %s, end: %s (ROI not yet implemented)" %(start, end)
            else:
                print "[INFO] software ROI inactive (ROI not yet implemented)"

    def __str__(self):
        if active:
            return "ROI start: %s end: %s" %(self.start, self.end)
        else:
            return "none"

    def roi(data):
        if self.active:
            return data[self.ystart:self.yend+1, self.xstart:self.xend+1]
        else:
            return data
