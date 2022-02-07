import cbor2
from dectris.compression import decompress
import numpy as np
import logging, os
import tifffile

logging.basicConfig()
log = logging.getLogger(__name__)


def decode_multi_dim_array(tag, column_major):
    dimensions, contents = tag.value
    if isinstance(contents, list):
        array = np.empty((len(contents),), dtype=object)
        array[:] = contents
    elif isinstance(contents, (np.ndarray, np.generic)):
        array = contents
    else:
        raise cbor2.CBORDecodeValueError("expected array or typed array")
    return array.reshape(dimensions, order="F" if column_major else "C")


def decode_typed_array(tag, dtype):
    if not isinstance(tag.value, bytes):
        raise cbor2.CBORDecodeValueError("expected byte string in typed array")
    return np.frombuffer(tag.value, dtype=dtype)


tag_decoders = {
    40: lambda tag: decode_multi_dim_array(tag, column_major=False),
    64: lambda tag: decode_typed_array(tag, dtype="u1"),
    65: lambda tag: decode_typed_array(tag, dtype=">u2"),
    66: lambda tag: decode_typed_array(tag, dtype=">u4"),
    67: lambda tag: decode_typed_array(tag, dtype=">u8"),
    68: lambda tag: decode_typed_array(tag, dtype="u1"),
    69: lambda tag: decode_typed_array(tag, dtype="<u2"),
    70: lambda tag: decode_typed_array(tag, dtype="<u4"),
    71: lambda tag: decode_typed_array(tag, dtype="<u8"),
    72: lambda tag: decode_typed_array(tag, dtype="i1"),
    73: lambda tag: decode_typed_array(tag, dtype=">i2"),
    74: lambda tag: decode_typed_array(tag, dtype=">i4"),
    75: lambda tag: decode_typed_array(tag, dtype=">i8"),
    77: lambda tag: decode_typed_array(tag, dtype="<i2"),
    78: lambda tag: decode_typed_array(tag, dtype="<i4"),
    79: lambda tag: decode_typed_array(tag, dtype="<i8"),
    80: lambda tag: decode_typed_array(tag, dtype=">f2"),
    81: lambda tag: decode_typed_array(tag, dtype=">f4"),
    82: lambda tag: decode_typed_array(tag, dtype=">f8"),
    83: lambda tag: decode_typed_array(tag, dtype=">f16"),
    84: lambda tag: decode_typed_array(tag, dtype="<f2"),
    85: lambda tag: decode_typed_array(tag, dtype="<f4"),
    86: lambda tag: decode_typed_array(tag, dtype="<f8"),
    87: lambda tag: decode_typed_array(tag, dtype="<f16"),
    1040: lambda tag: decode_multi_dim_array(tag, column_major=True),
}


def tag_hook(decoder, tag):
    tag_decoder = tag_decoders.get(tag.tag)
    return tag_decoder(tag) if tag_decoder else tag


def decompress_channel_data(channel):
    data = channel["data"]

    if isinstance(data, (np.ndarray, np.generic)):
        return data

    dimensions, encoded = data

    compression = channel["compression"]
    data_type = channel["data_type"]
    dtype = {"uint8": "u1", "uint16le": "<u2", "uint32le": "<u4"}[data_type]
    elem_size = {"uint8": 1, "uint16le": 2, "uint32le": 4}[data_type]

    if compression == "bslz4":
        decompressed = decompress(encoded, "bslz4-h5", elem_size=elem_size)
    elif compression == "lz4":
        decompressed = decompress(encoded, "lz4-h5")
    elif compression == "none":
        decompressed = encoded
    else:
        raise NotImplementedError(f"unknown compression: {compression}")

    return np.frombuffer(decompressed, dtype=dtype).reshape(dimensions)


def processMessage(frame, outDir):
    message = cbor2.loads(frame, tag_hook=tag_hook)
    
    if message["type"] == "start":
        log.info(f'**** start series {message["series_number"]}')
        for key, value in message.items():
            print(key, value)
 
        fname = f'{message["series_unique_id"]}_s{message["series_number"]:06d}_metaData.cbor'
        path = os.path.join(outDir, fname)
        with open(path, 'wb') as f:
            f.write(frame)
    
    elif message["type"] == "end":
        log.info(f'**** end series {message["series_number"]}')
    
    elif message["type"] == "image":
        fname = f'{message["series_unique_id"]}_{message["series_number"]:06d}_{message["image_number"]:06d}.cbor'
        path = os.path.join(outDir, fname)
        with open(path, 'wb') as f:
            f.write(frame)
            
        #for channel in message["channels"]:
        #    channel["data"] = decompress_channel_data(channel)

def processFile(fname, outDir):
    with open(fname, 'rb') as f:
        message = cbor2.loads(f.read(), tag_hook=tag_hook)
    
    os.makedirs(outDir, exist_ok=True)
    
    if message["type"] == "start":
        log.info(f'proocess series {message["series_unique_id"]} {message["series_number"]}')
        fname = f'{message["series_unique_id"]}_s{message["series_number"]:06d}_metaData.txt'
        path = os.path.join(outDir, fname)
        with open(path, 'w') as f:
            for key, value in message.items():
                f.write(f'{key} {value}\n')
    
    elif message["type"] == "end":
        log.info(f'proocess end of series {message["series_unique_id"]} {message["series_number"]}')
    
    elif message["type"] == "image":
        fname = f'{message["series_unique_id"]}_{message["series_number"]:06d}_{message["image_number"]:06d}.tif'
        path = os.path.join(outDir, fname)
    
        for channel in message["channels"]:
            channel["data"] = decompress_channel_data(channel)  
            thresholds = "_".join(map(str, channel["thresholds"]))  
            imgName = path.replace('.tif', f'_{thresholds}.tif')
            tifffile.imsave(imgName, channel["data"])       
            log.info(f'wrote {imgName}')     
