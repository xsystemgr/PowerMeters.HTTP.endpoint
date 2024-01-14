from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from chirpstack_api import integration
from google.protobuf.json_format import Parse
from struct import unpack

def decode_payload(payload):
    serial_number = unpack('>I', bytes.fromhex(payload[0:8]))[0]
    fragment_number = int(payload[8:10], 16)
    param_bytes = int(payload[10:12], 16)
    total_kwh = unpack('>f', bytes.fromhex(payload[12:20]))[0]
    voltage = unpack('>f', bytes.fromhex(payload[20:28]))[0]
    #current = unpack('>Q', bytes.fromhex(payload[28:52]))[0]
    frequency = unpack('>f', bytes.fromhex(payload[44:52] + '00000000'))[0]

    #power_factor = unpack('>f', bytes.fromhex(payload[36:44]))[0]

    return {
        "Serial Number": serial_number,
        "Fragment Number": fragment_number,
        "Number of Parameter Bytes": param_bytes,
        "Total kWh": total_kwh,
        "Voltage": voltage,
        #"Current": current,
       # "Power Factor": power_factor,
        "Frequency": frequency
    }

class Handler(BaseHTTPRequestHandler):
    # True -  JSON marshaler
    # False - Protobuf marshaler (binary)
    json = True

    def do_POST(self):
        self.send_response(200)
        self.end_headers()
        query_args = parse_qs(urlparse(self.path).query)

        content_len = int(self.headers.get('Content-Length', 0))
        body = self.rfile.read(content_len)

        if query_args["event"][0] == "up":
            self.up(body)

        elif query_args["event"][0] == "join":
            self.join(body)

        else:
            print("handler for event %s is not implemented" % query_args["event"][0])

    def up(self, body):
        up = self.unmarshal(body, integration.UplinkEvent())
        decoded_payload = decode_payload(up.data.hex())
        print("Uplink received from: %s with decoded payload: %s" % (up.device_info.dev_eui, decoded_payload))

    def join(self, body):
        join = self.unmarshal(body, integration.JoinEvent())
        print("Device: %s joined with DevAddr: %s" % (join.device_info.dev_eui, join.dev_addr))

    def unmarshal(self, body, pl):
        if self.json:
            return Parse(body, pl)

        pl.ParseFromString(body)
        return pl

httpd = HTTPServer(('', 5000), Handler)
httpd.serve_forever()
