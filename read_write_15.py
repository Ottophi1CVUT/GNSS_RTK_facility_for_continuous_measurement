import io
import pynmea2
import serial
import mariadb
import sys
import time

# Connect to MariaDB Platform
try:
    conn = mariadb.connect(
        user="otto",
        password="password1",
        host="localhost",
        database="Mereni_GNGGA"
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)
    
databaseName = "Mereni_GNGGA"
# Get Cursor
cur = conn.cursor()
print(f"Connected to DATABASE: {databaseName}")

# Define global variables
global UTCTime
global LAT
global LON
global Pos_Fix
global Sat_Used
global HDOP
global MSL_Altitude

global count
global failed_attempts

ser = serial.Serial("/dev/topblue", 38400, timeout=1)
sio = io.TextIOWrapper(io.BufferedRWPair(ser, ser))

count = 0
failed_attempts = 0
start_time = time.time()
while count < 15:  # Stop after 15 messages
    try:
        line = sio.readline()
        msg = pynmea2.parse(line)
        if isinstance(msg, pynmea2.GGA):
            if msg.gps_qual != 2:  # Check if Position Fix is not equal to 2
                print(f"Position not fixed. Elapsed time: {int(time.time() - start_time)} seconds.")
                failed_attempts += 1
                if failed_attempts >= 3:  # If failed attempts reach 3, end the program
                    print("Failed to obtain fix after 3 attempts. Exiting...")
                    sys.exit(0)
                continue  # Skip database insertion
            # Assign values to global variables
            UTCTime = float(msg.timestamp.strftime("%H%M%S.%f"))  # Convert timestamp to float
            LAT = float(msg.lat[:2]) + float(msg.lat[2:]) / 60.0  # Convert latitude to float
            if msg.lat_dir == 'S':
                LAT *= -1  # If latitude direction is 'S', make it negative
            LON = float(msg.lon[:3]) + float(msg.lon[3:]) / 60.0  # Convert longitude to float
            if msg.lon_dir == 'W':
                LON *= -1  # If longitude direction is 'W', make it negative
            Pos_Fix = msg.gps_qual  # Position fix
            Sat_Used = int(msg.num_sats)  # Number of satellites used
            HDOP = float(msg.horizontal_dil)  # Horizontal dilution of precision
            MSL_Altitude = float(msg.altitude)  # MSL Altitude

            # Insert information into the database
            try:
                query = f"INSERT INTO Mereni_GNGGA (UTCTime, LAT, N_S, LON, E_W, Pos_Fix, Sat_Used, HDOP, MSL_Altitude) VALUES ({UTCTime}, {LAT}, '{msg.lat_dir}', {LON}, '{msg.lon_dir}', {Pos_Fix}, {Sat_Used}, {HDOP}, {MSL_Altitude});"
                cur.execute(query)
                print(f"Data input was SUCCESSFUL. Elapsed time: {int(time.time() - start_time)} seconds.")
            except mariadb.Error as e: 
                print(f"Error: {e}")
            
            count += 1  # Increment count after inserting data
                
    except serial.SerialException as e:
        print('Device error:', e)
        break
    except pynmea2.ParseError as e:
        print('Parse error:', e)
        continue

# Close Connection
conn.commit()
conn.close()

print("Closing Connection")
