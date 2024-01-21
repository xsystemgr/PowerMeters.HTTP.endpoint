from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
from chirpstack_api import integration
from google.protobuf.json_format import Parse
from struct import unpack
import redis
import psycopg2
import configparser

class Handler(BaseHTTPRequestHandler):
    json = False

    config = configparser.ConfigParser()
    config.read('config.cfg')  # Αντικαταστήστε με το όνομα του πραγματικού αρχείου διαμόρφωσης

    redis_client = redis.StrictRedis(
        host=config.get('Redis', 'host'),
        port=config.getint('Redis', 'port'),
        decode_responses=True
    )

    database_connection = psycopg2.connect(
        host=config.get('PostgreSQL', 'host'),
        port=config.getint('PostgreSQL', 'port'),
        user=config.get('PostgreSQL', 'user'),
        password=config.get('PostgreSQL', 'password'),
        database=config.get('PostgreSQL', 'database')
    )
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
            print("Ο χειριστής για το συμβάν %s δεν έχει υλοποιηθεί" % query_args["event"][0])

    def up(self, body):
        up = self.unmarshal(body, integration.UplinkEvent())
        decoded_payload = decode_payload(up.data.hex())
        print("Λήψη Uplink από: %s με αποκωδικοποιημένα δεδομένα: %s" % (up.device_info.dev_eui, decoded_payload))

        # Αποθήκευση δεδομένων στον Redis
        redis_key = f"uplink:{up.device_info.dev_eui}"
        self.redis_client.set(redis_key, str(decoded_payload))

        # Προαιρετικά, αποθήκευση δεδομένων στη βάση δεδομένων
        self.store_in_database(up.device_info.dev_eui, decoded_payload)

    def join(self, body):
        join = self.unmarshal(body, integration.JoinEvent())
        print("Συσκευή: %s εντάχθηκε με DevAddr: %s" % (join.device_info.dev_eui, join.dev_addr))

        # Προαιρετικά, αποθήκευση συμβάντος ενταξης στον Redis ή τη βάση δεδομένων



    def store_in_database(self, dev_eui, decoded_payload):
        # Σύνδεση στη βάση δεδομένων
        database_connection = psycopg2.connect(
            host='192.168.2.14',
            port=5432,
            user='admin',
            password='PAASw0rd!',
            database='lwn'
        )

        cursor = database_connection.cursor()


        cursor.execute("SELECT EXISTS (SELECT 1 FROM lwn WHERE dev_eui = %s);", (dev_eui,))
        exists = cursor.fetchone()[0]

        if exists:

            query = '''
                UPDATE lwn
                SET serial_number = %s,
                    fragment_number = %s,
                    param_bytes = %s,
                    total_kwh = %s,
                    voltage = %s,
                    frequency = %s
                WHERE dev_eui = %s;
            '''
            data_tuple = (
                decoded_payload["Serial Number"],
                decoded_payload["Fragment Number"],
                decoded_payload["Number of Parameter Bytes"],
                decoded_payload["Total kWh"],
                decoded_payload["Voltage"],
                decoded_payload["Frequency"],
                dev_eui
            )
        else:

            query = '''
                INSERT INTO lwn (dev_eui, serial_number, fragment_number, param_bytes, total_kwh, voltage, current, power_factor, frequency)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            '''
            data_tuple = (
                dev_eui,
                decoded_payload["Serial Number"],
                decoded_payload["Fragment Number"],
                decoded_payload["Number of Parameter Bytes"],
                decoded_payload["Total kWh"],
                decoded_payload["Voltage"],
                decoded_payload["Frequency"],
                decoded_payload["Current"],
                decoded_payload["Power Factor"]
            )


        cursor.execute(query, data_tuple)
        database_connection.commit()


        cursor.close()
        database_connection.close()

    def unmarshal(self, body, pl):
        if self.json:
            return Parse(body, pl)

        pl.ParseFromString(body)
        return pl
def decode_payload(payload):
    serial_number = unpack('>I', bytes.fromhex(payload[0:8]))[0]
    fragment_number = int(payload[8:10], 16)
    param_bytes = int(payload[10:12], 16)
    total_kwh = unpack('>f', bytes.fromhex(payload[12:20]))[0]
    voltage = unpack('>f', bytes.fromhex(payload[20:28]))[0]
    current = unpack('<f', bytes.fromhex(payload[28:36]))[0]
    frequency = unpack('>f', bytes.fromhex(payload[44:52]))[0]
    power_factor = unpack('>f', bytes.fromhex(payload[36:44]))[0]

    #print("Μήκος δεδομένων serial:", len(payload))
    #print("Τμήμα για αποσυμπίεση serial:", payload[0:8])


    return {
        "Serial Number": serial_number,
        "Fragment Number": fragment_number,
        "Number of Parameter Bytes": param_bytes,
        "Total kWh": total_kwh,
        "Voltage": voltage,
        "Current": current,
        "Power Factor": power_factor,
        "Frequency": frequency
    }



# Δημιουργία ενός αντικειμένου HTTP server
httpd = HTTPServer(('', 5000), Handler)


httpd.serve_forever()
