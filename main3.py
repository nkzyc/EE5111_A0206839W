from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient
import random, time, datetime
import csv
import json

# A random programmatic shadow client ID.
SHADOW_CLIENT = "Mypas_rate_A0206839W"

# The unique hostname that &IoT; generated for
# this device.
HOST_NAME = "a2w96xsqrtn64r-ats.iot.ap-southeast-1.amazonaws.com"

# The relative path to the correct root CA file for &IoT;,
# which you have already saved onto this device.
ROOT_CA = "AmazonRootCA1.pem"

# The relative path to your private key file that
# &IoT; generated for this device, which you
# have already saved onto this device.
PRIVATE_KEY = "8c97f15050-private.pem.key"

# The relative path to your certificate file that
# &IoT; generated for this device, which you
# have already saved onto this device.
CERT_FILE = "8c97f15050-certificate.pem.crt"

# A programmatic shadow handler name prefix.
SHADOW_HANDLER = "Mypas_rate_A0206839W"


# Automatically called whenever the shadow is updated.
def myShadowUpdateCallback(payload, responseStatus, token):
    print()
    print('UPDATE: $aws/things/' + SHADOW_HANDLER +
          '/shadow/update/#')
    print("payload = " + payload)
    print("responseStatus = " + responseStatus)
    print("token = " + token)


# Create, configure, and connect a shadow client.
myShadowClient = AWSIoTMQTTShadowClient(SHADOW_CLIENT)
myShadowClient.configureEndpoint(HOST_NAME, 8883)
myShadowClient.configureCredentials(ROOT_CA, PRIVATE_KEY,
                                    CERT_FILE)
myShadowClient.configureConnectDisconnectTimeout(10)
myShadowClient.configureMQTTOperationTimeout(5)
myShadowClient.connect()

# Create a programmatic representation of the shadow.
myDeviceShadow = myShadowClient.createShadowHandlerWithName(
    SHADOW_HANDLER, True)

# *****************************************************
# Main script runs from here onwards.
# To stop running this script, press Ctrl+C.
# *****************************************************


file_path = "pas_rate.csv"
maNum = 'A0206839W'

title = [
'year', 'timestamp', 'MatricNumber','type', 'age', 'number_reported','number_passed','passing_rate']
content=[];

for i in range(8):
    title[i] = '\"' + title[i] + '\"'


with open("pas_rate.csv", "r", encoding="utf-8") as fp:

    # reader是一个迭代器
    reader = csv.reader(fp)
    # 执行一次next，指针跳过一位，可以不获取标题
    next(reader)
    for x in reader:
        content = [];
        tim = str(datetime.datetime.utcnow());
        content.append(str(title[0] + ':'))
        content.append(str('"' + x[0] + '",'))
        for i in range(1,8):
         content.append(str(title[i] + ':'))
         if i==1:
          content.append(str('"' + tim + '",'))
         elif i==2:
            content.append(str('"' + maNum + '",'))
         else:
             content.append(str('"' + x[i-2] + '",'))
        cc=''.join(content)[:-1]

        send = []
        send.append('{"state":{"reported":{'+cc+'}}}');
        send.append('\n');
        # print(send)
        send = ''.join(send)
        print(send)
        myDeviceShadow.shadowUpdate(send, myShadowUpdateCallback, 5)

    #time.sleep(10)
        time.sleep(10)

fp.close()
