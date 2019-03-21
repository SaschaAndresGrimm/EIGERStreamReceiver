"""
"""

from fileWriter import FileWriter
import tifffile
import threading
import numpy as np
import json
import os

from dectris import albula

__author__ = "SasG"
__date__ = "17/01/26"
__version__ = "0.0.1"
__reviewer__ = ""

class Stream2Albula(FileWriter):
    def __init__(self, basename, path, verbose=False):
        self.basename = basename
        self.path = path
        self._verbose = verbose
        self.ftype = ".tiff" #for pixelmask and flatfield
        self.series = 0
        self.metadata = {}
        self.openViewer()

        FileWriter().__init__(basename, path, self.ftype, verbose)

    def openViewer(self):
        self.mainFrame = albula.openMainFrame()
        self.subFrame = self.mainFrame.openSubFrame()
        return self.subFrame


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
            f.close()
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
            tifffile.imsave(path, data, metadata=self.metadata)
        else:
            raise IOError("file type %s not known. Allowed are .tiff|.dat" %ftype)

        print "[OK] wrote %s" %path

    def displayImage(self, data, series, frame):
        """
        display image in albula
        """
        img = albula.DImage().fromData(data)
        if self._verbose:
            print "[OK] sending image s%05d_f%05d to albula" %(series, frame)
        return self.subFrame.loadImage(img)

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

        if len(frames)==5:
            self.metadata["appendix"] = frames[4].bytes
        self.metadata["real_time"] = json.loads(frames[3].bytes)["real_time"]
        threading.Thread(target=self.displayImage,args=(data, header["series"], header["frame"])).start()
        return data
