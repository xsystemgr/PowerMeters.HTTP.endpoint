import psycopg2

# Συνδεση στη βαση δεδομενων
connection = psycopg2.connect(
    host='192.168.2.14',
    port=5432,
    user='admin',
    password='PAASw0rd!',
    database='lwn'
)

# Δημιουργία ενός καλεστέου
cursor = connection.cursor()

# Εκτέλεση ερωτήματος για να ελέγξετε αν υπάρχει η σχέση "lwn"
cursor.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'lwn');")
exists = cursor.fetchone()[0]

# Εάν η σχέση δεν υπάρχει, δημιουργήστε τη
if not exists:
    cursor.execute('''
        CREATE TABLE lwn (
            dev_eui VARCHAR(16) PRIMARY KEY,
            serial_number INTEGER,
            fragment_number INTEGER,
            param_bytes INTEGER,
            total_kwh FLOAT,
            voltage FLOAT,
            frequency FLOAT
        );
    ''')
    connection.commit()

# Κλείσιμο της σύνδεσης και του καλεστέου
cursor.close()
connection.close()
