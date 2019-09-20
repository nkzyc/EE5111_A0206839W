from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTShadowClient
import random, time, datetime

# A random programmatic shadow client ID.
SHADOW_CLIENT = "Mything_A0206839W"

# The unique hostname that &IoT; generated for
# this device.
HOST_NAME = "a2w96xsqrtn64r-ats.iot.ap-southeast-1.amazonaws.com"

# The relative path to the correct root CA file for &IoT;,
# which you have already saved onto this device.
ROOT_CA = "AmazonRootCA1.pem"

# The relative path to your private key file that
# &IoT; generated for this device, which you
# have already saved onto this device.
PRIVATE_KEY = "675e94272b-private.pem.key"

# The relative path to your certificate file that
# &IoT; generated for this device, which you
# have already saved onto this device.
CERT_FILE = "675e94272b-certificate.pem.crt"

# A programmatic shadow handler name prefix.
SHADOW_HANDLER = "Mything_A0206839W"


# Automatically called whenever the shadow is updated.
def myShadowUpdateCallback(payload, responseStatus, token):
    print()
    print('UPDATE: $aws/things/' + SHADOW_HANDLER +
          '/shadow/update/#')
    print("payload = " + payload)
    print("responseStatus = " + responseStatus)
    print("token = " + token)

def form_title ():
    title = ['id', 'timestamp', 'MatricNumber', 'cycle', 'os1', 'os2', 'os3'] + ['sensor' + str(i) for i in
                                                                                 range(1, 22)]
    for i in range(0, len(title)):
        title[i] = '\"' + title[i] + '\"'
    return title

#the joson form of data to send
def form_joson (content,x,title,tim,maNum):
    content.append(str(title[0] + ':'))
    content.append(str('"' + 'FD001_' + x[0] + '",'))

    for i in range(1, 28):
        content.append(str(title[i] + ':'))
        if i == 1:
            content.append(str('"' + tim + '",'))
        elif i == 2:
            content.append(str('"' + maNum + '",'))
        else:
            content.append(str('"' + x[i - 2] + '",'))
    cc = ''.join(content)[:-1]
    send = []
    send.append('{"state":{"reported":{' + cc + '}}}');
    send.append('\n');
    send = ''.join(send)
    return send

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


fp = open('train_FD001.txt', 'r')
maNum = 'A0206839W'
#title which satisfy the request by teacher
title=form_title ();

for reader in fp.readlines():
    x = reader.split(" ")
    #time
    tim = str(datetime.datetime.utcnow());
    #the content of the message
    content = [];
    #the joson form of data to send
    send=form_joson(content, x, title, tim, maNum)

    print(send)
    #send to the dynamnoDB
    myDeviceShadow.shadowUpdate(send, myShadowUpdateCallback, 5)

    time.sleep(10)


fp.close();

