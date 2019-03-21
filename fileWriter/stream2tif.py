"""
"""

from fileWriter import FileWriter
import tifffile
import threading
import numpy as np
import json
import os

try:
    from dectris import albula
    print "[INFO] using ALBULA API to handle images"
except:
    albula = None
    print "[INFO] using tifffile module to handle images"

__author__ = "SasG"
__date__ = "17/02/15"
__version__ = "0.0.2"
__reviewer__ = ""

class Stream2Tif(FileWriter):
    def __init__(self, basename, path, verbose=False, dtype=None):
        self.basename = basename
        self.path = path
        self._verbose = verbose
        self.ftype = ".tif"
        self.series = 0
        self.metadata = {}
        self.dtype = dtype

        FileWriter().__init__(basename, path, self.ftype, verbose)

    def saveConfig(self, frames, appendix=None):
        """
        save detector config as plain text
        """
        self.series = json.loads(frames[0].bytes)["series"]
        path = os.path.join(self.path, self.basename + "_%05d_config.json" %(self.series))
        data = json.loads(frames[1].bytes)
        if appendix:
            data["appendix"] = appendix.bytes
        self.metadata = data
        with open(path,"wb") as f:
            json.dump(data,f)
            f.close
        print "[OK] wrote %s" %path

    def saveTable(self, frames, name="", ftype=".dat"):
        """
        save pixel mask, flatfield or LUT
        """

        path = os.path.join(self.path, self.basename+"_%05d_%s%s" %(self.series,name,ftype))

        header = json.loads(frames[0].bytes)
        dtype = np.dtype(header["type"])

        data = np.reshape(np.fromstring(frames[1].bytes, dtype=dtype), header["shape"][::-1])

        if ftype == ".dat":
            np.savetxt(path, data)
        elif ftype == ".tif" or ftype == ".tiff":
            #if self.dtype: #do not reduce bit depth for int16 images!
            #    data = data.astype(self.dtype)
            tifffile.imsave(path, data, metadata=self.metadata)
        else:
            raise IOError("file type %s not known. Allowed are .tiff|.dat" %ftype)

        print "[OK] wrote %s" %path

    def saveImage(self, data, series, frame, metadata):
        """
        save image data as tiff
        """
        path = os.path.join(self.path, self.basename + "_%05d_%05d%s" %(series, frame+1, self.ftype))
        if albula:
            albula.DImageWriter.write(albula.DImage().fromData(data), path)
        else:
            tifffile.imsave(path, data, metadata=metadata)
        print "[OK] wrote %s" %path
        return path


    def __decodeHeader__(self, frames):
        """
        decode and process ZMQ header frames
        """

        header = json.loads(frames[0].bytes)
        if header["header_detail"]:
            if self._verbose:
                print "[OK] decode header ", header
        if header["header_detail"] is not "none":
            if len(frames) == 9:
                if self._verbose:
                    print "Appendix:", frames[8].bytes
                self.saveConfig(frames[0:2],frames[8])
            else:
                self.saveConfig(frames[0:2])
            if self._verbose:
                print "[OK] detector config:"
                for key, value in json.loads(frames[1].bytes).iteritems():
                    print "[*]" , key, value
        if header["header_detail"] == "all":
            if json.loads(frames[2].bytes)["htype"].startswith("dflatfield"):
                threading.Thread(target=self.saveTable,args=(frames[2:4],"flatfield",".tif")).start()
            if json.loads(frames[4].bytes)["htype"].startswith("dpixelmask"):
                threading.Thread(target=self.saveTable,args=(frames[4:6],"pixelmask",".tif")).start()
            if json.loads(frames[6].bytes)["htype"].startswith("dcountrate"):
                threading.Thread(target=self.saveTable,args=(frames[6:8],"countrate",".dat")).start()

    def __decodeImage__(self, frames):
        """
        decode ZMQ image frames and save as .tif
        """
        header = json.loads(frames[0].bytes)
        data = FileWriter().__decodeImage__(frames) # read back image data
        if self.dtype:
            data = data.astype(self.dtype)

        if len(frames)==5:
            self.metadata["appendix"] = frames[4].bytes
        self.metadata["real_time"] = json.loads(frames[3].bytes)["real_time"]
        threading.Thread(target=self.saveImage,args=(data, header["series"], header["frame"],self.metadata)).start()
        return data
