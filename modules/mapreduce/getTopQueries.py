import msgpack
import zlib
import snappy
import pykeyvi


def decode_value(value):
    """Decodes a cliqztionary value."""
    if value is None or len(value) == 0:
        return None
    elif len(value) > 1 and  value[0] == ' ':
        value = zlib.decompress(value[1:])
    return msgpack.loads(value)

count = 0
d = pykeyvi.Dictionary('pagemodels.kv-0')
items = d.GetAllItems()
for key, value in items:
   if (count ==20):
        break
   print decode_value(value)
   count+=1

