# Filename:     zway.py
# Authors:      apadin, mjmor, yabskbd, dvorva
# Start Date:   5/9/2016

"""
Interface for gathering data from ZWave devices running
on a ZWay server.

This python library is effectively a wrapper for the existing
JSON library published by ZWay. It requires that the server
software is already installed and running on the host machine.

It also requires knowledge of the devices on the network.
Hopefully this restriction will be lifted once I figure out
a way to parse this information from the devices page.

- Adrian Padin, 1/20/2017

UPDATE: Thanks to some clever scanning mechanisms and shortcuts,
this script no longer requires knowledge of the devices on the
Zway network. When the object is instantiated, it scans the
ZWave API for device information and saves it for later use.

- Adrian Padin, 2/7/2017

"""


#==================== LIBRARIES ====================#
import os
import json
import requests
import sys

# Change encoding to UTF-8 - certain sensors report non-ascii characters
reload(sys)
sys.setdefaultencoding('UTF8')

#==================== CLASSES ====================#

class Server(object):

    def __init__(self, host='localhost', port=8083, device_dict={}, username="", password=""):
        """
        Initialize connection to the network and obtain
        a list of available devices.
        """
        self.timeout = 5.0
        self.base_url = "http://{}:{}/ZWaveAPI/".format(host, port)

        # Check connection to the host
        try:
            self.generate_session(host, port, username, password)
            self._make_request("Data").json()
        except requests.ConnectionError as e:
            raise RuntimeError("Connection could not be established: destination host unreachable")
        except ValueError as e:
            raise RuntimeError("Connection could not be established: authentication required")            
        except BaseException as e:
            raise RuntimeError("Connection could not be established")

        # Obtain device dictionary
        if (device_dict == {}):
            self.update_devices()
        else:
            self.devices = device_dict

    def set_timeout(self, timeout):
        """Set the timeout period in seconds."""
        self.timeout = float(timeout)

    def update_devices(self):
        """
        Fetch device information from the server and generate device dictionary.
        Used on startup, as well as when adding or removing devices.
        """
        self.devices = {}
        acceptedClasses = ['48', '49', '50']
        devices_page = self._make_request("Run/devices").json()
        for device_id_base in devices_page:
            device_count = 0
            instances = devices_page[device_id_base]['instances']
            given_name = devices_page[device_id_base]['data']['givenName']['value']
            for instance_num in instances:
                commandClasses = instances[instance_num]['commandClasses']
                for commandClass in commandClasses:
                    if (commandClass in acceptedClasses):
                        for data_num in commandClasses[commandClass]['data']:
                            if (data_num.isdigit()):
                                data_dict = {}
                                data_dict['instance_num'] = instance_num
                                data_dict['command_class'] = commandClass
                                data_dict['data_num'] = data_num
                                if (commandClass == '48'):
                                    data_dict['url_suffix'] = 'level.value'
                                    data_dict['type'] = 'bool'
                                else:
                                    data_dict['url_suffix'] = 'val.value'
                                    data_dict['type'] = 'double'

                                device_id = "{}.{}".format(device_id_base, device_count)
                                self.devices[device_id] = {}
                                self.devices[device_id]['data'] = data_dict
                                device_count += 1

                                device_type = self.device_type(device_id).strip()
                                device_type = device_type.replace(' ', '_')
                                
                                name = device_id_base + '_' + device_type
                                self.devices[device_id]['name'] = name
                                self.devices[device_id]['header_name'] = name 
                                
                                try:
                                    given_name = given_name.replace(" ", "")
                                    header_name = given_name + '_' + device_type
                                    self.devices[device_id]['header_name'] = header_name 
                                except Exception as e:
                                    print str(e)
                                    print "ignoring device attr: " + device_type
                                    continue
                                print(header_name)

        return self.devices

    def device_IDs(self):
        """Return a sorted list of available device IDs"""
        return sorted(self.devices.keys(), key=float)

    def software_version(self):
        """Get the version of ZWay software running on this server"""
        command = self.base_url + "Data"
        Data_dict = self._make_request(command).json()
        return Data_dict['controller']['data']['softwareRevisionVersion']

    def battery_level(self, device_id):
        instance = self.devices[str(device_id)]['data']['instance_num']
        command = "Run/devices[{}].instances[0].Battery.data.last.value".format(device_id, instance)
        battery_percent = self._make_request(command).content
        return int(battery_percent)

    def get_data(self, device_id):
        """Fetch the data from this sensor given device ID and device information"""
        device_id = str(device_id)
        instance_num  = self.devices[device_id]['data']['instance_num']
        command_class = self.devices[device_id]['data']['command_class']
        data_num      = self.devices[device_id]['data']['data_num']
        data_type     = self.devices[device_id]['data']['type']
        suffix        = self.devices[device_id]['data']['url_suffix']
        device        = int(float(device_id))

        # Update the device
        command = "Run/devices[{}].instances[{}].commandClasses[{}].Get(sensorType=-1)"
        command = command.format(device, instance_num, command_class)
        self._make_request(command)

        # Retrieve data
        command = "Run/devices[{}].instances[{}].commandClasses[{}].data[{}].{}"
        command = command.format(device, instance_num, command_class, data_num, suffix)
        data = self._make_request(command).content

        if (data_type == 'bool'):
            data = 1 if (data == 'true') else 0

        return float(data)

    def device_type(self, device_id):
        """Return string representing the type of data from this device."""
        device_id = str(device_id)
        device        = int(float(device_id))
        instance_num  = self.devices[device_id]['data']['instance_num']
        command_class = self.devices[device_id]['data']['command_class']
        data_num      = self.devices[device_id]['data']['data_num']

        # Issue the command
        command = "Run/devices[{}].instances[{}].commandClasses[{}].data[{}].sensorTypeString.value"
        command = command.format(device, instance_num, command_class, data_num)
        return self._make_request(command).content

    def device_name(self, device_id):
        """Return string representing the name of this device."""
        return self.devices[device_id]['name']

    def header_names(self):
        """Return string list for device name for header ouput file."""
	device_ids = self.device_IDs()
	list_device = []
	for d_id in device_ids:
	    list_device.append(str(self.devices[d_id]['header_name']))
        return list_device
    
    def all_device_names(self):
        """Return string list representing the name of this devices."""
	device_ids = self.device_IDs()
	list_device = []
	for d_id in device_ids:
	    list_device.append(str(self.device_name(d_id)))
        return list_device

    def save_devices_to_file(self, fh):
        """Prints device dictionary to a file-like object in json format."""
        json.dump(self.devices, fh)

    def _make_request(self, command):
        """
        Returns the content of the page given by the command appended
        to the end of the base URL.
        The base URL is of the form: "http://YOUR.IP.GOES.HERE:PORT/ZWaveAPI/"

        This can be used as a back door for making any Zway request
        not currently supported. Handles disconnection errors and other issues.
        """
        num_attempts = 10
        for attempt in xrange(num_attempts):
            try:
                if (self.cookie == None):
                    page = requests.get(self.base_url + command, timeout=self.timeout)
                else:
                    page = requests.get(self.base_url + command, timeout=self.timeout, cookies=self.cookie)
            except requests.exceptions.ConnectionError:
                if (attempt == num_attempts-1):
                    raise Exception("server did not respond, connection is lost")
            except requests.exceptions.Timeout:
                # print("Timed out {}".format(attempt))
                if (attempt == num_attempts-1):
                    raise Exception("server Timeout")
            else:
                return page

    def generate_session(self, host, port, username, password):
        if username is "" and password is "":
            self.cookie = None
            return
        loginpage =  "http://{}:{}/ZAutomation/api/v1/login".format(host, port)
        headers = {
            'Accept':'application/json',
            'Content-Type':'application/json'
        }
        data = '{"form":true, "login":"'+username+'", "password":"'+password+'", "keepme":false, "default_ui":1}'
        #print loginpage, headers, data
        r = requests.post(loginpage, headers=headers, data=data)
        #print "Login response: ", r.text
        #print "Session cookie:"
        #print r.cookies.get_dict()
        self.cookie=r.cookies
