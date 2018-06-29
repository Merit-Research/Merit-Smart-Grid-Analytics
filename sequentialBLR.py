#!/usr/bin/env python
# Filename:     sequentialBLR.py
# Authors:      apadin, based on work by dvorva, yabskbd, mjmor, and mgkallit
# Start Date:   2017-01-10

"""Driver program for running the sequential BLR

This program is intended to replace all versions of
pi_seq_BLR* currently floating around. It was created
in order to help organize the repository after it became
public so that other viewers could better understand the
analysis process. It also implements several improvements
which would have been difficult to include in the
existing program. This includes:
 * Deques over lists or numpy arrays to hold the data
 * The Algo class to handle the BLR analysis
 * Use of json library to save and load settings
 * Simplification of argparse options
 * Better abstraction of data collection

- Adrian Padin, 1/10/2017

"""

#==================== LIBRARIES ====================#
import os
import sys
import time
import json
import argparse
import pymssql
import numpy as np

from modules.common import *
import modules.settings as settings
import modules.zway as zway
from modules.datalog import Datalog
from modules.algo import Algo
import modules.sound as Sound

#==================== FUNCTIONS ====================#

def get_features(zserver, sound=False):
    """Convenience function for getting a list of the features on the zserver"""
    features = []
    for key in zserver.device_IDs():
        features.append(zserver.get_data(key))
    if sound:
        sound = Sound.get_sound()
        features.append(sound)

    return features


def get_power(config_info):
    """Connects to the MS SQL database and retrieves the value to be used as
       total power consumption for the home"""
    user = config_info["database"]["credentials"]["username"]
    password = config_info["database"]["credentials"]["password"]
    host = config_info["database"]["credentials"]["host"]
    port = config_info["database"]["credentials"]["port"]
    database = config_info["database"]["credentials"]["database_name"]

    host = host + " " + port

    # Connect to database
    while True:
        try:
            cnx = pymssql.connect(server=host,
                                  user=user,
                                  password=password,
                                  database=database)
            break

        except Exception:
            print "Could not connect to power database."
            time.sleep(1)

    cursor = cnx.cursor()

    # Query the database
    Avg_over = 4
    qry_base = "SELECT TOP " + str(Avg_over)

    for data_column in config_info["database"]["table"]["data_columns"]:
        qry_base += "[" + data_column + "],"

    # strip off last comma
    qry_base = qry_base[:-1]

    qry = (qry_base + " FROM "
           + "[" + database + "].[dbo].["
           + config_info["database"]["table"]["name"] + "]"
           + " ORDER BY [") + (config_info["database"]["table"]["time_column"] + "] DESC")
    cursor.execute(qry)
    # Aggregate power to a single number
    final_power = 0
    for row in cursor:
        # this is where the value are, index the row returned
        # like row[0] for first data column, row[1] for second
        # data column, etc.
       final_power = final_power + max(row[0],0) + max(row[1],0) # + 4.068189 #offset shark_1 to zero
    cnx.close()
    return final_power/Avg_over



#==================== MAIN ====================#
def main(argv):

    #===== Initialization =====#
    folder = "/ne_data/"
    # Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('hostname', type=str, help="IP address or hostname of Z-way server host")
    parser.add_argument('-u', '--username', type=str, help="Username for Z-way server host")
    parser.add_argument('-p', '--password', type=str, help="Password for Z-way server host")
    parser.add_argument('-s', '--sound', action='store_true', help="use sound as a feature in analysis")
    parser.add_argument('-f', '--settings_file', type=str, help="load analysis settings from file")
    #parser.add_argument('-b', '--backup', action='store_true', help="start training on backup data")
    parser.add_argument('-t', '--time_allign', action='store_true', help="collect data only at times which are multiples of the granularity")
    parser.add_argument('-o', '--collect_only', action='store_true', help="collect data but do not run analysis")
    args = parser.parse_args(argv[1:])

    # Initialize Zway server
    host = args.hostname
    if args.username and args.password:
        zserver = zway.Server(host, username=args.username, password=args.password)
    else:
        zserver = zway.Server(host)

    # Use default settings or read settings from settings file
    if args.settings_file == None:
        settings_dict = {
            "prefix": "ne_data",
            "granularity": 60,
            "training_window": 120,
            "training_interval": 60,
            "ema_alpha": 1.0,
            "severity_omega": 1.0,
            "severity_lambda": 3.719,
            "auto_regression": 0.0
        }
    else:
        try:
            settings_dict = settings.load(args.settings_file)
        except Exception as error:
            print "Error reading settings file.", error
            print " "
            exit(1)

    # Initialize Algo class
    prefix = settings_dict['prefix']
    granularity = int(settings_dict['granularity'])
    training_window = int(settings_dict['training_window'])
    training_interval = int(settings_dict['training_interval'])
    ema_alpha = float(settings_dict['ema_alpha'])
    severity_omega = float(settings_dict['severity_omega'])
    severity_lambda = float(settings_dict['severity_lambda'])
    auto_regression = int(settings_dict['auto_regression'])

    #Feature Name Collection for file Header
    #feature_names = zserver.device_IDs()
    feature_names = zserver.header_names()
    print(feature_names)

    # Check if sound was enabled
    if args.sound:
        print("Sound Sensor Enabled")
        feature_names.append('Sound')
    # Number of Features +/- sound
    num_features = len(feature_names)

    print "Num features: ", num_features
    print "w = %.3f, L = %.3f" % (severity_omega, severity_lambda)
    print "alpha: %.3f" % ema_alpha

    algo = Algo(num_features, training_window, training_interval)
    algo.set_severity(severity_omega, severity_lambda)
    algo.set_EWMA(ema_alpha)

    # Two Datalogs: one for data and one for results
    feature_names.append('Total_power')
    print(feature_names)
    data_log = Datalog(folder, prefix, feature_names)

    results_header = ['target', 'prediction', 'anomaly']
    results_log = Datalog(folder, prefix + '_results', results_header)

    # Timing procedure
    granularity = settings_dict['granularity']
    goal_time = int(time.time())
    if args.time_allign:
        goal_time += granularity - (int(time.time()) % granularity)

    # Database Config Json
    with open('config.json') as config_json:
        config_dict = json.load(config_json)

    #===== Analysis =====#
    while(True):

        # Timing procedures
        while goal_time > time.time():
            time.sleep(0.2)
        goal_time = goal_time + granularity

        # Data collection
        #print "Recording sample at {}".format(goal_time)
        features = get_features(zserver, args.sound)
        power = get_power(config_dict)
        features.append(power)
        data_log.log(features[:], goal_time)

        # Do not run analysis if only collecting data
        if (args.collect_only): continue

        features = np.array(features).flatten()
        target, pred, anomaly, zscore = algo.run(features)

        if (anomaly != None):
            results_log.log([target, pred, float(anomaly)])
            print target, pred, anomaly
        else:
            print target, pred

    # Clean-up if necessary
    print "Ending analysis"


#==================== DRIVER ====================#
if __name__ == "__main__":
    main(sys.argv)

