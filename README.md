# EIGERStreamReceiver
basic EIGER zmq stream receiver


##Description

EIGERStreamReceiver.py listens to EIGER ZMQ messages and decodes the frames.
The data can be either saved as raw, json format, tif, cbf, h5 or
displayed in an ALBULA viewer window. Please see the corresponding file writer
modules for further information on the decoding process.


##Usage

EIGERStreamReceiver.py [-h] -i IP [-p PORT] [-v] [-f FILENAME]

Listen to stream interface and save data

optional arguments:
  -h, --help            show this help message and exit
  -i IP, --ip IP        EIGER host ip
  -p PORT, --port PORT  EIGER host port
  -v, --verbose         print some more messages
  -f FILENAME, --filename FILENAME
                        /path/to/file.ext with extension
                        [.bytes|.raw|.tiff|.tiff16|.cbf|.h5|.log|none] stream
                        data to ALBULA viewer window if filename is "albula"
