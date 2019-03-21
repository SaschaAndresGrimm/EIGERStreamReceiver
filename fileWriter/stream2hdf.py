"""
Decode EIGER ZMQ stream frames and save data as .h5.
The h5 structure is mimicked from the default EIGER file format.
The data arrays are stored as compressed containers according to
NIMAGESPERFILE, the meta data is stored as standard NEXUS tree.
Change NIMAGESPERFILE according to processing power and storge
capacity.

DISCLAIMER:
This code is build for demonstration purpose only. It is not meant
to be functioning, productive, efficient nor complete.

If you have any questions regarding the implementation of
the EIGER stream interface, please contact support@dectris.com.
"""
__author__ = "SasG"
__date__ = "16/11/22"
__version__ = "0.1.0"
__reviewer__ = ""

from fileWriter import FileWriter
import threading
import os
import h5py
import json
import numpy as np
import bitshuffle.h5
from datetime import datetime

#specify number of images per data file container
#this value might be adapted according to memory space and cpu power
NIMAGESPERFILE = 100

#data container compression for h5py. Can be ["lzf"|"gzip"|None]
#custom filters might be applied as well
COMPRESSION = "lzf"

# nodes mapping table: {node : {entry:/nexus/path, attr: {key:val, key2:val2}}}
# todo: implement key dtype
NXNODES = {     "entry" : {"entry" : "entry", "attrs" : {'NX_class': 'NXentry'}},
                "data" : {"entry" : "entry/data", "attrs" : {'NX_class': 'NXdata'}},
                "instrument" : {"entry" : "entry/instrument", "attrs" : {'NX_class': 'NXinstrument'}},
                "beam" : {"entry" : "entry/instrument/beam", "attrs" : {'NX_class': 'NXbeam'}},
                "detector" : {"entry" : "entry/instrument/detector", "attrs" : {'NX_class': 'NXdetector'}},
                "detectorSpecific" : {"entry" : "entry/instrument/detector/detectorSpecific", "attrs" : {'NX_class': 'NXcollection'}},
                "sample" : {"entry" : "entry/sample", "attrs" : {'NX_class': 'NXsample'}},
                "goniometer" : {"entry" : "entry/sample/goniometer", "attrs" : {'NX_class': 'NXtransformations'}},
                #"goniometer2" : {"entry" : "entry/instrument/detector/goniometer", "attrs" : {u'NX_class': 'NXtransformations'}}, # TODO: this must be changed
                "geometry" : {"entry" : "entry/instrument/detector/geometry", "attrs" : {'NX_class': 'NXgeometry'}}
            }
# stream mapping table: {stream key : {entry:/nexus/path, attr: {key:val, key2:val2}}}
PARAMTABLE = {
                "wavelength" : {"entry" : "entry/instrument/beam/incident_wavelength", "attrs" : {u'units': 'angstrom'}},
                "beam_center_x" : {"entry" : "entry/instrument/detector/beam_center_x", "attrs" : {u'units': 'pixel'}},
                "beam_center_y" : {"entry" : "entry/instrument/detector/beam_center_y", "attrs" : {u'units': 'pixel'}},
                "bit_depth_image" : {"entry" : "entry/instrument/detector/bit_depth_image", "attrs" : {}},
                "bit_depth_readout" : {"entry" : "entry/instrument/detector/bit_depth_readout", "attrs" : {}},
                "count_time" : {"entry" : "entry/instrument/detector/count_time", "attrs" : {u'units': 's'}},
                "countrate_correction_applied" : {"entry" : "entry/instrument/detector/countrate_correction_applied", "attrs" : {}},
                "description" : {"entry" : "entry/instrument/detector/description", "attrs" : {}},
                "auto_summation" : {"entry" : "entry/instrument/detector/detectorSpecific/auto_summation", "attrs" : {}},
                "calibration_type" : {"entry" : "entry/instrument/detector/detectorSpecific/calibration_type", "attrs" : {}},
                "compression" : {"entry" : "entry/instrument/detector/detectorSpecific/compression", "attrs" : {}},
                "countrate_correction_table" : {"entry" : "entry/instrument/detector/detectorSpecific/countrate_correction_table", "attrs" : {}},
                "countrate_correction_bunch_mode" : {"entry" : "entry/instrument/detector/detectorSpecific/countrate_correction_bunch_mode", "attrs" : {}},
                "countrate_correction_count_cutoff" : {"entry" : "entry/instrument/detector/detectorSpecific/countrate_correction_count_cutoff", "attrs" : {}},
                "data_collection_date" : {"entry" : "entry/instrument/detector/detectorSpecific/data_collection_date", "attrs" : {}},
                "detector_readout_period" : {"entry" : "entry/instrument/detector/detectorSpecific/detector_readout_period", "attrs" : {u'units': 's'}},
                "eiger_fw_version" : {"entry" : "entry/instrument/detector/detectorSpecific/eiger_fw_version", "attrs" : {}},
                "element" : {"entry" : "entry/instrument/detector/detectorSpecific/element", "attrs" : {}},
                "flatfield" : {"entry" : "entry/instrument/detector/detectorSpecific/flatfield", "attrs" : {}},
                "frame_count_time" : {"entry" : "entry/instrument/detector/detectorSpecific/frame_count_time", "attrs" : {u'units': 's'}},
                "frame_period" : {"entry" : "entry/instrument/detector/detectorSpecific/frame_period", "attrs" : {u'units': 's'}},
                "module_bandwidth" : {"entry" : "entry/instrument/detector/detectorSpecific/module_bandwidth", "attrs" : {}},
                "nframes_sum" : {"entry" : "entry/instrument/detector/detectorSpecific/nframes_sum", "attrs" : {}},
                "nimages" : {"entry" : "entry/instrument/detector/detectorSpecific/nimages", "attrs" : {}},
                "nsequences" : {"entry" : "entry/instrument/detector/detectorSpecific/nsequences", "attrs" : {}},
                "ntrigger" : {"entry" : "entry/instrument/detector/detectorSpecific/ntrigger", "attrs" : {}},
                "number_of_excluded_pixels" : {"entry" : "entry/instrument/detector/detectorSpecific/number_of_excluded_pixels", "attrs" : {}},
                "photon_energy" : {"entry" : "entry/instrument/detector/detectorSpecific/photon_energy", "attrs" : {u'units': 'eV'}},
                "pixel_mask" : {"entry" : "entry/instrument/detector/detectorSpecific/pixel_mask", "attrs" : {}},
                "roi_mode" : {"entry" : "entry/instrument/detector/detectorSpecific/roi_mode", "attrs" : {}},
                "software_version" : {"entry" : "entry/instrument/detector/detectorSpecific/software_version", "attrs" : {}},
                "summation_nimages" : {"entry" : "entry/instrument/detector/detectorSpecific/summation_nimages", "attrs" : {}},
                "test_mode" : {"entry" : "entry/instrument/detector/detectorSpecific/test_mode", "attrs" : {}},
                "trigger_mode" : {"entry" : "entry/instrument/detector/detectorSpecific/trigger_mode", "attrs" : {}},
                "x_pixels_in_detector" : {"entry" : "entry/instrument/detector/detectorSpecific/x_pixels_in_detector", "attrs" : {}},
                "y_pixels_in_detector" : {"entry" : "entry/instrument/detector/detectorSpecific/y_pixels_in_detector", "attrs" : {}},
                "detector_distance" : {"entry" : "entry/instrument/detector/detector_distance", "attrs" : {u'units': 'm'}},
                "detector_number" : {"entry" : "entry/instrument/detector/detector_number", "attrs" : {}},
                "detector_readout_time" : {"entry" : "entry/instrument/detector/detector_readout_time", "attrs" : {u'units': 's'}},
                "efficiency_correction_applied" : {"entry" : "entry/instrument/detector/efficiency_correction_applied", "attrs" : {}},
                "flatfield_correction_applied" : {"entry" : "entry/instrument/detector/flatfield_correction_applied", "attrs" : {}},
                "frame_time" : {"entry" : "entry/instrument/detector/frame_time", "attrs" : {u'units': 's'}},
                "detector_orientation" : {"entry" : "entry/instrument/detector/geometry/orientation", "attrs" : {u'NX_class': 'NXorientation'}},
                "value" : {"entry" : "entry/instrument/detector/geometry/orientation/value", "attrs" : {}},
                "detector_translation" : {"entry" : "entry/instrument/detector/geometry/translation", "attrs" : {u'NX_class': 'NXtranslation'}},
                "distances" : {"entry" : "entry/instrument/detector/geometry/translation/distances", "attrs" : {}},
                "two_theta" : {"entry" : "entry/instrument/detector/goniometer/two_theta", "attrs" : {u'units': 'degree'}},
                "two_theta_end" : {"entry" : "entry/instrument/detector/goniometer/two_theta_end", "attrs" : {u'units': 'degree'}},
                "two_theta_increment" : {"entry" : "entry/instrument/detector/goniometer/two_theta_increment", "attrs" : {u'units': 'degree'}},
                "two_theta_range_average" : {"entry" : "entry/instrument/detector/goniometer/two_theta_range_average", "attrs" : {u'units': 'degree'}},
                "two_theta_range_total" : {"entry" : "entry/instrument/detector/goniometer/two_theta_range_total", "attrs" : {u'units': 'degree'}},
                "two_theta_start" : {"entry" : "entry/instrument/detector/goniometer/two_theta_start", "attrs" : {u'units': 'degree'}},
                "pixel_mask_applied" : {"entry" : "entry/instrument/detector/pixel_mask_applied", "attrs" : {}},
                "sensor_material" : {"entry" : "entry/instrument/detector/sensor_material", "attrs" : {}},
                "sensor_thickness" : {"entry" : "entry/instrument/detector/sensor_thickness", "attrs" : {u'units': 'm'}},
                "threshold_energy" : {"entry" : "entry/instrument/detector/threshold_energy", "attrs" : {u'units': 'eV'}},
                "virtual_pixel_correction_applied" : {"entry" : "entry/instrument/detector/virtual_pixel_correction_applied", "attrs" : {}},
                "x_pixel_size" : {"entry" : "entry/instrument/detector/x_pixel_size", "attrs" : {u'units': 'm'}},
                "y_pixel_size" : {"entry" : "entry/instrument/detector/y_pixel_size", "attrs" : {u'units': 'm'}},
                "chi" : {"entry" : "entry/sample/goniometer/chi", "attrs" : {u'units': 'degree'}},
                "chi_end" : {"entry" : "entry/sample/goniometer/chi_end", "attrs" : {u'units': 'degree'}},
                "chi_increment" : {"entry" : "entry/sample/goniometer/chi_increment", "attrs" : {u'units': 'degree'}},
                "chi_range_average" : {"entry" : "entry/sample/goniometer/chi_range_average", "attrs" : {u'units': 'degree'}},
                "chi_range_total" : {"entry" : "entry/sample/goniometer/chi_range_total", "attrs" : {u'units': 'degree'}},
                "chi_start" : {"entry" : "entry/sample/goniometer/chi_start", "attrs" : {u'units': 'degree'}},
                "kappa" : {"entry" : "entry/sample/goniometer/kappa", "attrs" : {u'units': 'degree'}},
                "kappa_end" : {"entry" : "entry/sample/goniometer/kappa_end", "attrs" : {u'units': 'degree'}},
                "kappa_increment" : {"entry" : "entry/sample/goniometer/kappa_increment", "attrs" : {u'units': 'degree'}},
                "kappa_range_average" : {"entry" : "entry/sample/goniometer/kappa_range_average", "attrs" : {u'units': 'degree'}},
                "kappa_range_total" : {"entry" : "entry/sample/goniometer/kappa_range_total", "attrs" : {u'units': 'degree'}},
                "kappa_start" : {"entry" : "entry/sample/goniometer/kappa_start", "attrs" : {u'units': 'degree'}},
                "omega" : {"entry" : "entry/sample/goniometer/omega", "attrs" : {u'units': 'degree'}},
                "omega_end" : {"entry" : "entry/sample/goniometer/omega_end", "attrs" : {u'units': 'degree'}},
                "omega_increment" : {"entry" : "entry/sample/goniometer/omega_increment", "attrs" : {u'units': 'degree'}},
                "omega_range_average" : {"entry" : "entry/sample/goniometer/omega_range_average", "attrs" : {u'units': 'degree'}},
                "omega_range_total" : {"entry" : "entry/sample/goniometer/omega_range_total", "attrs" : {u'units': 'degree'}},
                "omega_start" : {"entry" : "entry/sample/goniometer/omega_start", "attrs" : {u'units': 'degree'}},
                "phi" : {"entry" : "entry/sample/goniometer/phi", "attrs" : {u'units': 'degree'}},
                "phi_end" : {"entry" : "entry/sample/goniometer/phi_end", "attrs" : {u'units': 'degree'}},
                "phi_increment" : {"entry" : "entry/sample/goniometer/phi_increment", "attrs" : {u'units': 'degree'}},
                "phi_range_average" : {"entry" : "entry/sample/goniometer/phi_range_average", "attrs" : {u'units': 'degree'}},
                "phi_range_total" : {"entry" : "entry/sample/goniometer/phi_range_total", "attrs" : {u'units': 'degree'}},
                "phi_start" : {"entry" : "entry/sample/goniometer/phi_start", "attrs" : {u'units': 'degree'}}
                }

class Stream2Hdf(FileWriter):
    """
    h5 file writer class, extends FileFriwter
    """
    def __init__(self, basename, path, verbose=False):
        """
        create Stream2Hdf instance.
        args:
            basename:   file basename
            path:       file path
            verbose:    verbosity
        """
        self.basename = basename # file basename
        self.path = path # file path
        self.ftype = ".h5" # file extension

        self._verbose = verbose # verbosity
        FileWriter().__init__(self.basename, path, self.ftype, verbose) # FileWrite init procedure

        self.nimagesPerFile = NIMAGESPERFILE # images per h5 container. adapt this value according to memory and cpu capaciy.

        self.__initParams__()

    def __initParams__(self):
        """
        initialize/reset the filewriter parameters to defaulte values
        """
        self.__series__ = None # series id
        self.__frameID__ = [] # frame id array
        self.__nimages__ = 0 # number of collected images in series
        self.__imageIndex__ = 0 # image container id
        self.__dataBuffer__ = [] # data buffer which stores image data arrays
        self.__startTime__ = None # start time of acquisition
        self.master = None # master file name. Is created when receiving a dheader message

    def __createMaster__(self, frames):
        """
        create master hdf file from a dheader frame.
        name structure: /path/basename_seriesId_master.h5
        arg: dheader frames
        return: master filename
        """

        header = json.loads(frames[0].bytes) # header dict

        self.__imageIndex__ = 0 # reset image index
        self.__nimages__ = 0 # reset number of collected images
        self.__dataBuffer__ = [] # reset data buffer
        self.__series__ = header["series"] # series id
        self.__frameID__ = [] # frame ID array
        self.__startTime__ = datetime.now()


        # build master filename /path/fname_<series_ID>_master.h5
        self.master = os.path.join(self.path,self.basename+"_{}_master{}".format(self.__series__,self.ftype))

        # write master file with empty group entries, set NX attributes
        with h5py.File(self.master, "w", libver='earliest') as f:
            for key, entry in NXNODES.iteritems():
                try:
                    group = f.create_group(entry["entry"])
                except ValueError: # group already exists
                    group = f[entry["entry"]]
                for attr, v in entry["attrs"].iteritems():
                    group.attrs.create(attr,v)
            print "[OK] wrote %s" %self.master
            f.close()

        return self.master

    def __writeConfig__(self, frame):
        """
        write config entries from the header frames to the master file.
        arg: frame (zmq config frame)
        return: number of written config params
        """
        data = json.loads(frame.bytes)

        with h5py.File(self.master, "a", libver='earliest') as f:
            for key, value in data.iteritems():
                self.__setParam__(f, key, value)

            self.__setParam__(f, "compression", str(COMPRESSION)) # set compression used in h5py
            f.close()
            print "[OK] wrote %d parameters to %s" %(len(data)+1, self.master)

        return len(data)

    def __writeCorrection__(self, frames):
        """
        decode and write flatfiled, pixel mask, or lut to master .h5 file.
        args:
            frames: info and data blob zmq frames
        returns: data numpy array
        """
        htypes = {"dflatfield-1.0":"flatfield", "dpixelmask-1.0":"pixel_mask", "dcountrate_table-1.0":"countrate_correction_table"}
        header = json.loads(frames[0].bytes) # header dict
        key = htypes[header["htype"]] # get correction type name

        dtype = np.dtype(header["type"]) # data type
        data = np.reshape(np.fromstring(frames[1].bytes, dtype=dtype), header["shape"][::-1]) # decode data

        with h5py.File(self.master, "a", libver='earliest') as f:
                self.__setParam__(f, key, data)
                f.close()

        return data

    def __appendData__(self, data=None):
        """
        append data array to data buffer and write data buffer to file
        if image numbers exceeds self.nimagesPerFile.
        args: data, 3 dimensional np.array
        """
        # append data to data buffer stack
        if data is not None:
            # append data to buffer
            self.__dataBuffer__.append(data)
            self.__nimages__ += 1

        # check if container must be written
        if len(self.__dataBuffer__) >= self.nimagesPerFile:
            threading.Thread(target=self.__writeData__,args=(self.__dataBuffer__,)).start()
            self.__dataBuffer__ = []

    def __writeData__(self, data):
        """
        write data buffer to h5 container file and link container to master file.
        return: data file name

        TODO: implement better compression: compression=bitshuffle.h5.H5FILTER,
                                            compression_opts=(block_size = 8, bitshuffle.h5.H5_COMPRESS_LZ4))
        """
        #pack data to container
        if data:
            self.__imageIndex__ += 1 # increment data file index
            dname = "data_{:06d}".format(self.__imageIndex__) # data file basename
            dfile = self.basename+"_{}_{}{}".format(self.__series__,dname,self.ftype) # data file name
            filePath = os.path.join(self.path, dfile) # data file path

            # write data file
            with h5py.File(filePath, "w", libver='earliest') as f:
                    data = np.vstack(data)
                    dset = f.create_dataset("/entry/data/data", data=data, chunks=(1,) + data.shape[-2:], compression=COMPRESSION)

                    f["entry"].attrs.create('NX_class', 'NXentry')
                    f["entry/data"].attrs.create('NX_class','NXdata')
                    dset.attrs.create("image_nr_high", np.shape(data)[0], dtype="int32")
                    dset.attrs.create("image_nr_low", 1, dtype="int32")
                    f.close()
                    print "[OK] wrote %s" %filePath

            # link data container to master file if exists and set bitdepth entry
            if self.master:
                with h5py.File(self.master, "a", libver='earliest') as f:

                    f["/entry/data/"+dname] = h5py.ExternalLink(dfile, "/entry/data/data/")
                    f["/entry/data"].attrs.create("NX_class", "NXdata")
                    f["/entry"].attrs.create("NX_class", "NXentry")
                    self.__setParam__(f, "bit_depth_image", data.dtype.itemsize * 8) # set bit depth
                    f.close()

            return dfile

        else:
            return None # return None if no data file was written

    def __decodeImage__(self, frames):
        """
        decode ZMQ image frames and pass np array to the write function.
        args: frames, image info and data blob frames
        return: np data array
        """
        data = FileWriter().__decodeImage__(frames) # read back image data

        if len(frames)==5 and self._verbose: # image appendix.
            # TODO: maybe append to nexus tree. Discuss with AndF.
            print "[*] appendix: %s\n" %frames[4].bytes

        header = json.loads(frames[0].bytes)

        if not self.__series__: # if series id not given e.g. if arm was not detected
            self.__series__ = header["series"]

        self.__frameID__.append(header["frame"])

        self.__appendData__(data=np.array(data,ndmin=3)) # handle data, must be 3 dim
        return data

    def __decodeHeader__(self, frames):
        """
        decode and process ZMQ header frames and pass framses to corresponding module, either
        createMaster, writeConfig or writeCorrection.
        arg: frames, ZMQ header message frames
        return: True/False
        """
        header = json.loads(frames[0].bytes) #header dict

        if header["header_detail"]:
            if self._verbose:
                print "[OK]received header ", header
            self.__createMaster__(frames)
            if header["header_detail"] is not "none":
                self.__writeConfig__(frames[1])
            if header["header_detail"] == "all":
                self.__writeCorrection__(frames[2:4]) # flatfield
                self.__writeCorrection__(frames[4:6]) # pixelmask
                self.__writeCorrection__(frames[6:8]) # countrate_correction_table
            if len(frames) == 9:
                if self._verbose:
                    print "[*] Appendix: ", json.loads(frames[8].bytes) #TODO discuss how to handle appendix
            return True
        else:
            print "[WARNING] Could not decode dheader frames"
            return False

    def __decodeEndOfSeries__(self, frames):
        """
        Decode end of series message and write down image buffer.
        args: frames, ZMQ EndOfSeries Frame(s)
        return: True
        """
        FileWriter().__decodeEndOfSeries__(frames)
        self.__writeData__(self.__dataBuffer__) # write image buffer
        self.__calcAngles__()  # calculate and write goniometer angles

        print self.__getStatistics__() # print series statistics
        self.__initParams__() # reset variables

        return True

    def __getStatistics__(self):
        """
        print statistic message including number of decoded images.
        return: statistics string
        """
        # TODO implement more useful information
        msg = ""
        if self.__frameID__: # if images were collected
            dropped = set(range(self.__frameID__[len(self.__frameID__)-1])[1:]) - set(self.__frameID__) # dropped frames IDs
        else:
            dropped = []

        dlim = "\n#######################\n"
        msg += dlim + "series Statistics:"
        msg += "\nmaster filename: %s" %self.master
        msg += "\nseries: {}\ncollected images: {}".format(self.__series__, self.__nimages__)
        msg += "\ndropped frames: %d" %len(dropped)
        #msg += "\ndropped frame IDs: %s" %list(dropped)
        try:
            uptime = datetime.now() - self.__startTime__
            msg += "\nelapsed time: {}".format(uptime)
        except:
            pass
        msg += dlim

        return msg

    def __calcAngles__(self):
        """
        Calculate angles array corresponding to default EIGER h5 data set and write
        angle, angle_end, angle_range_total into master file. Call this method at end
        of series. Assumption: all images were received, none dropped.
        """
        if self.master:
            for angle in ["chi","kappa","omega","phi","two_theta"]:
                try:
                    with h5py.File(self.master, "a", libver='earliest') as f:
                        # calculate angles
                        start = f[PARAMTABLE[angle + "_start"]["entry"]].value
                        increment = f[PARAMTABLE[angle + "_increment"]["entry"]].value
                        if self.__frameID__:
                            data = start + np.multiply(self.__frameID__, increment)
                        else:
                            data = [start]
                        data_end = start + increment

                        # write params to h5
                        self.__setParam__(f, angle, data)
                        self.__setParam__(f, angle+"_end", data_end)
                        self.__setParam__(f, angle+"_range_total", data[-1]-data[0])
                        self.__setParam__(f, angle+"_range_average", increment)

                        # delete not used parameters from h5
                        self.__delParam__(f, angle+"_start")
                        self.__delParam__(f, angle+"_increment")

                        if self._verbose:
                            print "[OK] angle: %s, start: %f, end: %f, n: %d" %(angle, start, data[-1], self.__nimages__)
                        f.close()
                except Exception as e:
                    print "[ERROR] %s" %s

    def __setParam__(self, filehandler, key, data):
        """
        Write parameter, value, and attributes from mapping table to file.
        arguments:
        <filehandler>: h5py filehandler to .h5 file
        <key>: key name in NXNODES or PARAMTABLE mapping table
        <data>: data value
        returns: 1 if successful, 0 if parameter not found
        """
        if key in NXNODES:
            param = NXNODES[key]
        elif key in PARAMTABLE:
            param = PARAMTABLE[key]
        else:
            print "[WARNING] could not find %s in mapping tables" %key
            return 0

        try:
            dset = filehandler.create_dataset(param["entry"], data=data)
            for attr, v in param["attrs"].iteritems():
                dset.attrs.create(attr, v)
            if self._verbose:
                print "[*] wrote parameter %s" %key
        except Exception as e:
            print "[WARNING] could not write %s to %s" %(key, filehandler.filename)
            return 0

        return 1

    def __delParam__(self, filehandler, key):
        """
        Delete parameter from file according to mapping table entry.
        arguments:
        <filehandler>: h5py filehandler to .h5 file
        <key>: key name in NXNODES or PARAMTABLE mapping table
        returns: 1 if successful, 0 if not
        """
        if key in NXNODES:
            param = NXNODES[key]
        elif key in PARAMTABLE:
            param = PARAMTABLE[key]
        else:
            print "[WARNING] could not find %s in mapping tables" %key
            return 0

        try:
            del filehandler[param["entry"]]
        except Exception as e:
            print "[WARNING] could not delete %s in %s" %(key, filehandler.filename)
            return 0

        return 1
