"""
Directly map received ZMQ frames to files without decoding.
This interface is meant for performance testing.

DISCLAIMER:
This code is build for demonstration pupose only. It is not meant
to be productive, nor efficient or complete.

If you have any questions regarding the implementation of
the EIGER stream interface, please contact support@dectris.com.
"""
from fileWriter import FileWriter
import os
import threading

__author__ = "SasG"
__date__ = "16/11/22"
__version__ = "0.0.2"
__reviewer__ = ""

class Stream2Bytes(FileWriter):
    def __init__(self, basename, path, verbose=False):
        self.basename = basename # file basename
        self.path = path # file path
        self.__verbose__ = verbose # verbosity
        self.ftype = ".bytes" # file extension

        FileWriter().__init__(basename, path, self.ftype, verbose) # filewriter init routine

        self.filename = os.path.join(path, basename) # create filename
        self.index = 0 # file index

    def decodeFrames(self, frames):
        """
        write frame bytes to file with name format /path/basename_id.bytes
        arg: frames, list of ZMQ frames
        return: file index number
        """
        threading.Thread(target=self.__processFrames,args=(frames,)).start()
        return

    def __processFrames(self, frames):
        for i in range(len(frames)):
            self.index +=1
            filename = "{}.{:06d}".format(self.filename,self.index)
            with open(filename, "wb") as f:
                f.write(frames[i].bytes)
                f.close()
        return self.index
