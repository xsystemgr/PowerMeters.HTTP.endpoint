from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from chirpstack_api import integration
from google.protobuf.json_format import Parse
import struct
import binascii

class Handler(BaseHTTPRequestHandler):
    # True - JSON marshaler
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
            print("Handler for event %s is not implemented" % query_args["event"][0])

    def up(self, body):
        up = self.unmarshal(body, integration.UplinkEvent())
        decoded_data_step2 = self.decode_step2(up.data.hex())
        if decoded_data_step2:
            serial_number, num_bytes, total_kwh, crc = decoded_data_step2
            print("Uplink received from: %s with payload:" % up.device_info.dev_eui)
            print("Serial Number: %s" % serial_number)
            print("Number of Data Bytes Sent: %s" % num_bytes)
            print("Total kWh: %f" % total_kwh)
            print("CRC: %s" % crc)

    def join(self, body):
        join = self.unmarshal(body, integration.JoinEvent())
        print("Device: %s joined with DevAddr: %s" % (join.device_info.dev_eui, join.dev_addr))


    def unmarshal(self, body, pl):
        if self.json:
            return Parse(body, pl)

        pl.ParseFromString(body)
        return pl

    def decode_step2(self, data):
        if len(data) < 21:
            print("Invalid data length")
            return

        serial_number = struct.unpack('>I', bytes.fromhex(data[:8]))[0]
        num_bytes = int(data[8], 16)
        total_kwh = struct.unpack('>f', bytes.fromhex(data[9:17]))[0]
        crc = data[17:21]

        return serial_number, num_bytes, total_kwh, crc


httpd = HTTPServer(('', 5000), Handler)
httpd.serve_forever()
