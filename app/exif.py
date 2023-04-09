from io import BytesIO

from PIL import Image, ExifTags, TiffImagePlugin
from PIL.ExifTags import TAGS, GPSTAGS


def get_image(content):
    return Image.open(BytesIO(content))


def get_exif_tags(exif):
    return _normalize({
        ExifTags.TAGS[k]: v
        for k, v in exif.items()
        if k in ExifTags.TAGS
    })


def get_exif_gps(exif):
    for key, value in TAGS.items():
        if value == 'GPSInfo':
            break
    gps_info = exif.get_ifd(key)
    return _normalize({
        GPSTAGS.get(key, key): value
        for key, value in gps_info.items()
    })


def get_exif_ifd(exif):
    for key, value in TAGS.items():
        if value == 'ExifOffset':
            break
    info = exif.get_ifd(key)
    return _normalize({
        TAGS.get(key, key): value
        for key, value in info.items()
    })


def _normalize(exif):
    result = {}
    for k, v in exif.items():
        if type(v) is TiffImagePlugin.IFDRational:
            try:
                result[str(k)] = float(v)
            except Exception as e:
                print('❌ TiffImagePlugin.IFDRational float(v):', e)
                result[str(k)] = str(v)
        elif type(v) is tuple:
            result[str(k)] = []
            for i in v:
                try:
                    result[str(k)].append(float(i))
                except Exception as e:
                    print('❌ tuple float(i):', e)
                    result[str(k)].append(str(i))
        else:
            result[str(k)] = v

    return result
