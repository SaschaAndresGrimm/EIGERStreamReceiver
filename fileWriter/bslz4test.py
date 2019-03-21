import numpy as np
import bitshuffle
import json
import tifffile
import cbf

def readBSLZ4(datafile, headerfile):
    """
    unpack bitshuffle-lz4 compressed frame and return np array image data
    frame: zmq data blob frame
    shape: image shape
    dtype: image data type
    """
    with open(datafile,"r") as f:
        data = f.read()
    with open(headerfile,"r") as f:
        header = json.loads(f.read())
        shape = header["shape"]
        dtype = np.dtype(header["type"])

    blob = np.fromstring(data[12:], dtype=np.uint8)
    # blocksize is big endian uint32 starting at byte 8, divided by element size
    blocksize = np.ndarray(shape=(), dtype=">u4", buffer=data[8:12])/dtype.itemsize
    print blocksize, dtype.itemsize
    imgData = bitshuffle.decompress_lz4(blob, shape[::-1], dtype, blocksize)
    print "[OK] unpacked {0} bytes of bs-lz4 data".format(len(imgData))
    return imgData

if __name__ == "__main__":
    data = readBSLZ4("/Users/sascha.grimm/Downloads/lcp_100hz/000078_002.raw",
                "/Users/sascha.grimm/Downloads/lcp_100hz/000078_001.raw")
    tifffile.imsave("bslz4_16bit.tiff",data)
    tifffile.imsave("bslz4_32bit.tiff",data.astype("uint32"))
    cbf.write("bslz4_32bit.cbf",data.astype("uint32"))
