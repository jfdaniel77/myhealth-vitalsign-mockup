import boto3

import paho.mqtt.client as mqtt 
from urllib.parse import urlparse
from datetime import datetime, date
from json import dumps, loads
from random import randrange, randint, uniform
from time import time, sleep
from botocore.config import Config

my_config = Config(
    region_name = 'ap-southeast-1',
    signature_version = 'v4',
    retries = {
        'max_attempts': 10,
        'mode': 'standard'
    }
)

# Constants
LOGPREFIX = "myHealth"
TOPIC = 'vitalsign'

def on_connect(client, userdata, flags, rc):
    print("{} - On Connect - client: {}".format(LOGPREFIX, client))
    print("{} - On Connect - userdata: {}".format(LOGPREFIX, userdata))
    print("{} - On Connect - flags: {}".format(LOGPREFIX, flags))
    print("{} - On Connect - rc: {}".format(LOGPREFIX, rc))
    
def on_message(client, obj, msg):
    print("{} - On Message - client: {}".format(LOGPREFIX, client))
    print("{} - On Message - obj: {}".format(LOGPREFIX, obj))
    print("{} - On Message - msg topic: {}".format(LOGPREFIX, msg.topic))
    print("{} - On Message - msg qos: {}".format(LOGPREFIX, msg.qos))
    print("{} - On Message - msg payload: {}".format(LOGPREFIX, msg.payload))
    
def on_publish(client, obj, mid):
    print("{} - On Publish - client: {}".format(LOGPREFIX, client))
    print("{} - On Publish - obj: {}".format(LOGPREFIX, obj))
    print("{} - On Publish - mid: {}".format(LOGPREFIX, mid))
    
def on_subscribe(client, obj, mid, granted_qos):
    print("{} - On Subscribe - client: {}".format(LOGPREFIX, client))
    print("{} - On Subscribe - obj: {}".format(LOGPREFIX, obj))
    print("{} - On Subscribe - mid: {}".format(LOGPREFIX, mid))
    print("{} - On Subscribe - granted_qos: {}".format(LOGPREFIX, granted_qos))
    
def on_log(client, obj, level, string):
    print("{} - On Log - client: {}".format(LOGPREFIX, client))
    print("{} - On Log - obj: {}".format(LOGPREFIX, obj))
    print("{} - On Log - level: {}".format(LOGPREFIX, level))
    print("{} - On Log - string: {}".format(LOGPREFIX, string))

mqttc = mqtt.Client()
mqttc.on_message = on_message
mqttc.on_connect = on_connect
mqttc.on_publish = on_publish
mqttc.on_subscribe = on_subscribe

def main():
    
    cb_flag = get_parameter_value("serverless-vitalsign-cb-flag")
    
    while True:
        
        if cb_flag and cb_flag == 'OFF':
            print("{} - Circuit Bracker Flag is set".format(LOGPREFIX))
            sleep(60)
            continue
        else:
            # Get initial data
            list_patient = get_intial_data()
            list_data = generate_random_vital_sign(list_patient)
            print("{} - Data has been generated.".format(LOGPREFIX))
            
            # Connect
            url_str = get_parameter_value("serverless-mqtt-url")
            url = urlparse(url_str)
            mqttc.username_pw_set(get_parameter_value("serverless-mqtt-user"), get_parameter_value("serverless-mqtt-pwd"))
            mqttc.connect(url.hostname, url.port)
            
            print("{} - Connection has been established.".format(LOGPREFIX))
            for data in list_data:
                mqttc.publish(TOPIC, dumps(data))
                print("{} - Publish: {}".format(LOGPREFIX, data))
                sleep(1)
        
        print("{} - Take 300 seconds sleep..z.zz.zzz.zzzz".format(LOGPREFIX))
        sleep(300)
    
def get_parameter_value(key):
    print("{} - Get Parameter key: {}".format(LOGPREFIX, key))
    ssm_client = boto3.client("ssm", config=my_config)
    value = ssm_client.get_parameter(Name=key, WithDecryption=False)
    return value.get("Parameter").get("Value")
    
def get_intial_data():
    print("{} - Get Initial Data".format(LOGPREFIX))
    list_data_patient = []
    with open('patient-data.csv', 'r') as file:
        list_data_patient = file.read().splitlines()
    print("{} - Length of patient data: {}".format(LOGPREFIX, len(list_data_patient)))
    return list_data_patient
    
def generate_random_vital_sign(list_patient):
    print("{} - Generate Random Vital Sign".format(LOGPREFIX))
    len_list = len(list_patient)
    
    count = 0
    done_patient = []
    list_data = []
    
    while count < 200:
        condition = randint(0,2)
        val_rand_patient = randint(0, len_list-1)
        temp_patient = list_patient[val_rand_patient]
        patient = temp_patient.split(",")
        
        if patient[0] in done_patient:
            continue
        else:
            done_patient.append(patient[0])
        
        val_bp_high = randint(115, 160)
        val_bp_low = randint(75, 110)
        val_hr = randint(80, 120)
        val_rr = randint(15,30)
        val_os = randint(93, 99)
        val_bt =  round(uniform(36, 37.5), 1)
        
        val_bw = float(patient[2])
        if condition == 1:
            val_bw = round(val_bw + randint(0, 2), 2)
        else:
            val_bw = round(val_bw - randint(0, 2), 2)
            
        val_bh = float(patient[3])
        val_time = int(round(time() * 1000))
            
        data = {
            "USER_ID": patient[0],
            "BP": "{}/{}".format(str(val_bp_high), str(val_bp_low)),
            "BT": val_bt,
            "HR": val_hr,
            "RR": val_rr,
            "OS": val_os,
            "BW": val_bw,
            "BH": val_bh,
            "datetime": val_time
        }
        # print("{} - Data: {}".format(LOGPREFIX, data))
        list_data.append(data)
        count = count + 1
    return list_data

if __name__ == "__main__":
    print("Mock Smart device is starting...")
    main()