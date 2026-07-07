import asyncore
import binascii
import json
import threading
from datetime import datetime
import paho.mqtt.client as mqtt
import socket
from datetime import date

from influxdb import InfluxDBClient

full_packet_list = []
ServerActive = True
Serverip = '185.222.242.249'
Serverport = 5029
broker_address = "192.168.0.109"
broker_port = 1883
responsePacket = ''
response2 = ''

with open('/data/options.json', 'r') as config_file:    config = json.load(config_file)
DATABASE_PORT = config.get('database_port', '8086')  # Default to '8086' if not set
USERNAME_DATABASE = config.get('username_database', 'default_username')
PASSWORD_DATABASE = config.get('password_database', 'default_password')
INTERNAL_BACKUP_DATABASE_NAME = config.get('internal_backup_database_name', 'default_backup_db')
INTERNAL_DATABASE_NAME = config.get('internal_database_name', 'default_internal_db')
DATABASE_IP = config.get('database_ip', '127.0.0.1')
measurement = config.get('measurement', 'default_measurement')

def ConvertKSA(packet):
    hour = packet[46:48]
    print(int(hour, 16))
    newtime = str(hex(int(hour, 16) )).replace("0x", "")
    if len(newtime) == 1:
        newtime = "0" + newtime
    newpacket = packet[:46] + newtime + packet[48:]
    return newpacket


def Checked_SavedHolding_Database():
    client = InfluxDBClient(DATABASE_IP, DATABASE_PORT, USERNAME_DATABASE, PASSWORD_DATABASE,
                            INTERNAL_BACKUP_DATABASE_NAME)
    result = client.query('SELECT *  FROM ' + str(INTERNAL_BACKUP_DATABASE_NAME) + '."autogen".' + str(measurement))
    length = len(list(result.get_points()))
    if length != 0:
        print("hold data length ", length)
        return True
    else:
        return False


def Send_Saved_Database():
    client = InfluxDBClient(DATABASE_IP, DATABASE_PORT, USERNAME_DATABASE, PASSWORD_DATABASE,
                            INTERNAL_BACKUP_DATABASE_NAME)
    result = client.query('SELECT *  FROM ' + str(INTERNAL_BACKUP_DATABASE_NAME) + '."autogen".' + str(measurement))
    data = list(result.get_points())
    for point in data:
        SendPacketToServer(str(point["Packet"]))
        client.delete_series(database=INTERNAL_BACKUP_DATABASE_NAME, measurement=measurement, tags={"id": point["id"]})


def Save_IndexNum(index):
    textfile = open("IndexNum.txt", "w")
    textfile.write(str(index))
    textfile.close()


def Load_IndexNum():
    text_file = open("IndexNum.txt", "r")
    lines = text_file.readlines()
    Nlist = [i.replace("\n", "").strip() for i in lines]
    return int(Nlist[0])


def Set_IndexNumber():
    Save_IndexNum(0)


def SendPacketHoldingDataBase(packet):
    from influxdb import InfluxDBClient
    client = InfluxDBClient(DATABASE_IP, DATABASE_PORT, USERNAME_DATABASE, PASSWORD_DATABASE,
                            INTERNAL_BACKUP_DATABASE_NAME)
    try:
        index = Load_IndexNum()
    except:
        Set_IndexNumber()
        index = Load_IndexNum()

    DataPoint = [
        {
            "measurement": measurement,
            "tags": {
                "id": index
            },
            "fields": {
                "Packet": packet
            }
        }
    ]
    index += 1
    Save_IndexNum(index)
    client.write_points(DataPoint)


def SendPacketToServer(packet_hex: str) -> bool:
    """
    Sends one packet to the Skarpt server.
    Returns True if sent successfully, False otherwise.
    On failure, it saves the original packet to the holding database.
    """
    original_packet = packet_hex  # keep for backup
    ksa_packet = ConvertKSA(packet_hex)

    try:
        print("sending to skarpt server")
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)  # optional timeout
        s.connect((Serverip, Serverport))
        s.sendall(binascii.unhexlify(ksa_packet))
        s.close()
        return True
    except Exception as e:
        print(f"server {Serverip} error while sending packet: {e}")
        # HERE: only if server connection/send failed -> send to holding DB
        SendPacketHoldingDataBase(original_packet)
        return False

def mqttsend(jsonlist, sensoridlist):
    print("creating new instance")
    client = mqtt.Client("P1")  # create new instance
    print("connecting to broker")
    client.connect(broker_address, broker_port)  # connect to broker
    # client.username_pw_set(username="homeassistant", password="yayeeheed8eezaechiwu4thahbaij2eiki0eim8Bo1chahbatief4Ohs1mait0Ph")
    # client.subscribe("LastAttendance")
    for i in range(len(jsonlist)):
        client.publish(measurement + "/" + str(sensoridlist[i]), str(jsonlist[i]))


def Update_ACK(Packetindex):
    print("updating ack")
    global responsePacket, response2
    # str = '@CMD,*000000,@ACK,'+Packetindex+'#,#'
    str1 = '@ACK,' + Packetindex + '#'
    str1 = str1.encode('utf-8')
    responsePacket = str1.hex()
    now_utc = datetime.now()
    dt = now_utc.replace(year=now_utc.year - 4)
    response2 = "Server UTC time:" + str(dt)[:19]
    response2 = response2.encode('utf-8')
    response2 = response2.hex()


def ConvertRTCtoTime(RTC):
    Year, Month, Day, Hours, Min, Sec = RTC[0:2], RTC[2:4], RTC[4:6], RTC[6:8], RTC[8:10], RTC[10:12]
    Year, Month, Day, Hours, Min, Sec = int(Year, 16), int(Month, 16), int(Day, 16), int(Hours, 16), int(Min, 16), int(
        Sec, 16)
    print("Date is ", Year, "/", Month, "/", Day)
    print("Time is ", Hours, "/", Min, "/", Sec)
    Date = str(Year) + "/" + str(Month) + "/" + str(Day)
    Time = str(Hours) + "/" + str(Min) + "/" + str(Sec)
    # return  Year, Month, Day, Hours, Min, Sec
    return Date, Time


def TempFun(temp):
    sign = ''
    hexadecimal = temp
    end_length = len(hexadecimal) * 4
    hex_as_int = int(hexadecimal, 16)
    hex_as_binary = bin(hex_as_int)
    padded_binary = hex_as_binary[2:].zfill(end_length)
    normalbit = padded_binary[0]
    postitive = padded_binary[1]
    value = padded_binary[2:]
    if str(normalbit) == '0':
        pass
    else:
        return 255

    if str(postitive) == '0':
        sign = '+'
    else:
        sign = '-'

    if sign == '+':
        return str(int(value, 2) / 10)

    else:
        return "-" + str(int(value, 2) / 10)

def HumFun_1byte(hum_hex: str) -> str:
    """
    1 byte humidity (unit = 1%)
    Examples:
      '2D'   -> '45'
      'FF'   -> '0' (no humidity)
    """
    hum_hex = hum_hex.strip().lower()

    # No humidity
    if hum_hex == "ff":
        return "0"

    if len(hum_hex) != 2:
        raise ValueError(f"Invalid 1-byte humidity: '{hum_hex}'")

    value = int(hum_hex, 16)   # 0x2D = 45
    return str(value)


def HumFun_2bytes(hum_hex: str) -> str:
    """
    2 bytes humidity (unit = 0.1%)
    Examples:
      '02CF' -> '71.9'
      'FFFF' -> '0' (no humidity)
    """
    hum_hex = hum_hex.strip().lower()

    # No humidity
    if hum_hex == "ffff":
        return "0"

    if len(hum_hex) != 4:
        raise ValueError(f"Invalid 2-byte humidity: '{hum_hex}'")

    raw = int(hum_hex, 16)   # e.g. 0x02CF = 719
    value = raw / 10.0       # 71.9 %

    # Return as string, one decimal place
    return f"{value:.1f}"

def TestServerConnection():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((Serverip, Serverport))
        s.send(binascii.unhexlify("00"))
        return True
    except:
        return False


def logic(packet):
    print("logic thread ...")

    if TestServerConnection():
        print("skarpt server is active (test connection ok)")
        sent_ok = SendPacketToServer(packet)

        if sent_ok and Checked_SavedHolding_Database():
            print("sending hold data")
            threading.Thread(target=Send_Saved_Database, args=[]).start()
    else:
        print("server test failed, saving packet to holding DB")
        SendPacketHoldingDataBase(packet)


def ConvertPacketIntoElemets(packet):
    threading.Thread(target=logic, args=[packet]).start()

    sensorfound = False
    Sensorhexlist = []

    # Packet index (before CRC & 0D0A)
    Packetindex = packet[-12:-8]
    print(Packetindex)
    Update_ACK(str(int(Packetindex, 16)))

    # TAG / sensor info length (bytes)
    Packetsensorlength = packet[76:80]
    if Packetsensorlength == "0000":
        GatwayId = packet[24:40]
        RTC = packet[40:52]
        date, time = ConvertRTCtoTime(RTC)
        GatewayBattary = int(packet[68:72], 16) / 100
        GatewayPower = int(packet[72:76], 16) / 100
        print("No TAGs in this packet, gateway-only frame:", GatwayId, date, time, GatewayBattary, GatewayPower)
        return 0

    sensor_data_len = int(Packetsensorlength, 16)  # e.g. 0x014d = 333
    if sensor_data_len != 0:
        sensorfound = True

        # Number of sensors declared in header
        NumberOfSensors = int(packet[82:84], 16)
        print("Number Of Sensors", NumberOfSensors, "Sensor(s)")

        # Length per sensor in bytes
        length_per_sensor_bytes = int(packet[84:86], 16)
        length_per_sensor_hex = length_per_sensor_bytes * 2  # hex chars

        # Calculate max sensors that actually fit in this section
        payload_bytes = sensor_data_len - 3  # minus (type + count + len_per)
        max_sensors_by_length = payload_bytes // length_per_sensor_bytes

        sensors_to_parse = min(NumberOfSensors, max_sensors_by_length)

        if NumberOfSensors != max_sensors_by_length:
            print("⚠ mismatch: header says",
                  NumberOfSensors, "but length allows",
                  max_sensors_by_length)

        sensor_data_start = 86

        for idx in range(sensors_to_parse):
            start = sensor_data_start + idx * length_per_sensor_hex
            end = start + length_per_sensor_hex
            Sensorhexlist.append(packet[start:end])

    GatwayId = packet[24:40]
    print(GatwayId)

    RTC = packet[40:52]
    date, time = ConvertRTCtoTime(RTC)

    GatewayBattary = int(packet[68:72], 16) / 100
    print("Battary of Gateway ", GatewayBattary, "Volt")

    GatewayPower = int(packet[72:76], 16) / 100
    print("Power of Gateway ", GatewayPower, "Volt")
    sensorType=packet[80:82]
    print("sensor Type is :",sensorType)

    print("sensorfound:", sensorfound, "count:", len(Sensorhexlist))
    ConvertSensorsToReadings(
        GatwayId, date, time,
        GatewayBattary, GatewayPower,
        sensors_to_parse,   # use actual parsed count
        Sensorhexlist,
        sensorType
    )


def ConvertSensorsToReadings(GatwayId, date, time, GatewayBattary, GatewayPower, NumberOfSensors, Sensorhexlist,sensorType):
    sensor_id_list = []
    sensor_temp_list = []
    sensor_hum_list = []
    sensor_battary_list = []
    jsonlist = []
    dectionarylist = []
    for sensor_block in Sensorhexlist:
        sensor_id_list.append(sensor_block[0:8])
        sensor_battary_list.append(int(sensor_block[10:14], 16) / 1000)
        sensor_temp_list.append(TempFun(sensor_block[14:18]))

        if sensorType == "01":
            # 1 byte humidity, 1% unit
            hum_hex = sensor_block[18:20]
            sensor_hum_list.append(HumFun_1byte(hum_hex))
        else:
            # 2 byte humidity, 0.1% unit
            hum_hex = sensor_block[18:22]
            sensor_hum_list.append(HumFun_2bytes(hum_hex))

    print(sensor_id_list)
    print(sensor_temp_list)
    print(sensor_hum_list)
    print(sensor_battary_list)
    for index in range(NumberOfSensors):
        jsonname = {"GatewayId": GatwayId, "GatewayBattary": GatewayBattary, "GatewayPower": GatewayPower, "Date": date,
                    "Time": time,
                    "Sensorid": sensor_id_list[index], "SensorBattary": sensor_battary_list[index],
                    "temperature": sensor_temp_list[index], "humidity": sensor_hum_list[index]
                    }
        dectionarylist.append(jsonname)
        print(json.dumps(jsonname))
        jsonlist.append(json.dumps(jsonname))
    # mqttsend(jsonlist,sensor_id_list)
    del jsonname, jsonlist, sensor_id_list, sensor_temp_list, sensor_hum_list, sensor_battary_list, GatwayId, date, time, GatewayBattary, GatewayPower, NumberOfSensors, Sensorhexlist
    SendToInternalDataBase(dectionarylist)


'''
def SendToInternalDataBaseToken (dectionarylist):
    bucket = "n"
    client = InfluxDBClient(url="http://localhost:8086",
                            token="n9cd2F9mYZcfhDE7892UzJv7xP38SSyQG9ybQRsYmGp6Bbv6OnbrGl5QGygzsZuzaCQTX-10w1EqY4axQNEzVg==",
                            org="skarpt")

    write_api = client.write_api(write_options=SYNCHRONOUS)
    query_api = client.query_api()
    for i in dectionarylist :
        p = Point("Tzone").tag("gateway",i["Sensorid"]).field("temperature", float(i["temperature"])).time(datetime(2021, 12, 20, 0, 0), WritePrecision.US)
        write_api.write(bucket=bucket, record=p)
        print("database saved read")
'''


def BuildJsonDataBase(Date, Time, Temp, Hum, Battery, GateWayID, SensorID):
    listofdate = Date.split("/")
    Year, Month, day = listofdate
    listoftime = Time.split("/")
    Hour, Mins, Sec = listoftime
    Year = "20" + Year
    ReadingTime = datetime(int(Year)+4, int(Month), int(day), int(Hour), int(Mins), int(Sec)).isoformat() + "Z"
    JsonData = [
        {
            "measurement": measurement,
            "tags": {
                "SensorID": SensorID,
                "GatewayID": GateWayID
            },
            "time": ReadingTime,
            "fields": {
                "Temperature": float(Temp),
                "Humidity": float(Hum),
                "Battery": float(Battery)
            }
        }
    ]
    return JsonData


def SendToInternalDataBase(dectionarylist):
    from influxdb import InfluxDBClient
    client = InfluxDBClient(DATABASE_IP, DATABASE_PORT, USERNAME_DATABASE, PASSWORD_DATABASE, INTERNAL_DATABASE_NAME)
    for i in dectionarylist:
        DataPoint = BuildJsonDataBase(i["Date"], i["Time"], i["temperature"], i["humidity"], i["SensorBattary"],
                                      i["GatewayId"], i["Sensorid"])
        client.write_points(DataPoint)
    del dectionarylist


def check_packet(data):
    return True
    check_code = data[-8:- 4]
    # The range is from Protocol type to Packet index(include Protocol type and Packet index)
    hex_data = data[8:-8]
    our_model = PyCRC.CRC_16_MODBUS
    crc = CRC.CRC(hex_data, our_model)

    if check_code.lower() == crc.lower():
        return True
    else:
        return False


def preprocess_packet(data):
    global full_packet_list

    data = str(binascii.hexlify(data).decode())
    print(data)
    data = data.strip()
    if data.startswith("545a") and data.endswith("0d0a"):
        full_packet_list = []
        if check_packet(data):
            ConvertPacketIntoElemets(data)
        return [binascii.unhexlify(responsePacket.strip()), binascii.unhexlify(response2.strip())]
    elif data.endswith("0d0a") and not data.startswith("545a") and full_packet_list:
        collecting_packet = ''
        for packet_part in full_packet_list:
            collecting_packet += packet_part
        collecting_packet += data
        if check_packet(collecting_packet):
            ConvertPacketIntoElemets(collecting_packet)
        full_packet_list = []
        return [binascii.unhexlify(responsePacket.strip()), binascii.unhexlify(response2.strip())]
    else:
        full_packet_list.append(data)

    return 0


class EchoHandler(asyncore.dispatcher_with_send):

    def handle_read(self):
        data = self.recv(8192)
        if data:
            try:
                send_list = preprocess_packet(data)
                if send_list != 0:
                    for i in send_list:
                        self.send(i)
            except Exception as e:
                import traceback
                print("\n❌ Error in handle_read:")
                print("   Exception:", e)
                print("   Type:", type(e))
                print("   Traceback:")
                traceback.print_exc()


class EchoServer(asyncore.dispatcher):

    def __init__(self, host, port):
        asyncore.dispatcher.__init__(self)
        self.create_socket()
        self.set_reuse_addr()
        self.bind((host, port))
        self.listen(5)

    def handle_accepted(self, sock, addr):
        print('Incoming connection from %s' % repr(addr))
        handler = EchoHandler(sock)


server = EchoServer('', 2000)
asyncore.loop()
