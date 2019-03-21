"""
TODO:
-comment and clean up
-flatfield not as integer!!!
-header data not stored
"""

from fileWriter import FileWriter
import threading
import cbf
import lz4, bitshuffle
import numpy as np
import json
import os


try:
    from dectris import albula
    print "[INFO] using ALBULA API to handle images"
except:
    albula = None
    print "[INFO] using PSI cbf module to handle images"


__author__ = "SasG"
__date__ = "16/11/22"
__version__ = "0.0.2"
__reviewer__ = ""

class Stream2Cbf(FileWriter):
    def __init__(self, basename, path, verbose=False):
        self.basename = basename
        self.path = path
        self._verbose = verbose
        self.ftype = ".cbf"
        self.series = 0
        self.metadata = {}

        FileWriter().__init__(basename, path, self.ftype, verbose)

    def saveConfig(self, frames):
        """
        save detector config as plain text
        """
        if len(frames) == 9:
            appendix = frames[8]
        else:
            appendix = None
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
        elif ftype == ".cbf":
            cbf.write(path, data) # self.__getHeader__())
        else:
            raise IOError("file type %s not known. Allowed are .cbf|.dat" %ftype)

        print "[OK] wrote %s" %path

    def saveImage(self, data, series, frame):
        """
        save image data as cbf
        """
        path = os.path.join(self.path, self.basename + "_%05d_%05d%s" %(series, frame+1, self.ftype))
        if albula:
            albula.DImageWriter.write(albula.DImage().fromData(data), path)
        else:
            cbf.write(path, data, header = self._createHeader(self.metadata))
        print "[OK] wrote %s" %path
        return path

    def _createHeader(self, metadata):
        """
        return cbf header string from meta data
        """
        header = ""
        for key, value in metadata.iteritems():
            header += "# %s: %s\n" %(key, value)
        return header

    def __decodeHeader__(self, frames):
        """
        decode and process ZMQ header frames
        """
        header = json.loads(frames[0].bytes)
        if header["header_detail"]:
            if self._verbose:
                print "[OK] decode header ", header
        if header["header_detail"] is not "none":
            if len(frames) == 9 and self._verbose:
                print "[*] Appendix:", frames[8].bytes

            self.saveConfig(frames[0:2])
            if self._verbose:
                print "[OK] detector config:"
                for key, value in json.loads(frames[1].bytes).iteritems():
                    print "[*] ", key, value

        if header["header_detail"] == "all":
            if json.loads(frames[2].bytes)["htype"].startswith("dflatfield"):
                threading.Thread(target=self.saveTable,args=(frames[2:4],"flatfield",".cbf")).start()
            if json.loads(frames[4].bytes)["htype"].startswith("dpixelmask"):
                threading.Thread(target=self.saveTable,args=(frames[4:6],"pixelmask",".cbf")).start()
            if json.loads(frames[6].bytes)["htype"].startswith("dcountrate"):
                threading.Thread(target=self.saveTable,args=(frames[6:8],"countrate",".dat")).start()

    def __decodeImage__(self, frames):
        """
        decode ZMQ image frames and save as .cbf
        """
        data = FileWriter().__decodeImage__(frames) # read back image data
        info = json.loads(frames[1].bytes)
        header = json.loads(frames[0].bytes)
        if len(frames)==5:
            self.metadata["appendix"] = frames[4].bytes
        self.metadata["real_time"] = json.loads(frames[3].bytes)["real_time"]
        threading.Thread(target=self.saveImage,args=(data, header["series"], header["frame"])).start()
        return data
