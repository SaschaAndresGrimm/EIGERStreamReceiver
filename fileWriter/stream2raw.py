"""
write bytes of zmq frames to .json without decoding contents.
"""

from fileWriter import FileWriter
import threading
import numpy as np
import json
import os

__author__ = "SasG"
__date__ = "16/11/22"
__version__ = "0.0.2"
__reviewer__ = ""

class Stream2Raw(FileWriter):
    def __init__(self, basename, path, verbose=False):
        self.basename = basename
        self.path = path
        self.__verbose__ = verbose
        self.ftype = ".raw"

        FileWriter().__init__(basename, path, self.ftype, verbose)

    def frames2raw(self,frames):
        """
        write frame bytes to file
        """
        header = json.loads(frames[0].bytes)
        fname = "%s_%s_%05d" %(self.basename,header["htype"],header["series"])
        if "frame" in header.keys(): # add frame id
            fname += "_%05d" %header["frame"]

        for i in range(len(frames)):
            path = os.path.join(self.path, fname + "_ZMQframe%05d%s" %(i, self.ftype))
            with open(path, "wb") as f:
                f.write(frames[i].bytes)
                f.close()
                print "[OK] wrote file %s" %path

    def __decodeHeader__(self, frames):
        """
        decode and process ZMQ header frames
        """
        header = json.loads(frames[0].bytes)
        if header["header_detail"] and self.__verbose__:
            print "[OK] decode header ", header
        if header["header_detail"] is not "none":
            if self.__verbose__:
                print "[OK] detector config:"
                for key, value in json.loads(frames[1].bytes).iteritems():
                    print "[*] ", key, value
        if len(frames) == 9:
            if self.__verbose__:
                print "Appendix:", frames[8].bytes
        threading.Thread(target=self.frames2raw,args=(frames,)).start()


    def __decodeEndOfSeries__(self, frames):
        if self.__verbose__:
            print "[OK] decode end of series ", json.loads(frames[0].bytes)
        threading.Thread(target=self.frames2raw,args=(frames,)).start()

    def __decodeImage__(self, frames):
        """
        decode ZMQ image frames
        """
        if self.__verbose__:
            print "[OK] decode image"
        threading.Thread(target=self.frames2raw,args=(frames,)).start()
