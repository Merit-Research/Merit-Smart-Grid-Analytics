# Merit Smart Grid Analytics
### ISGADA: Integrated Smart Grid Analytics for Detecting Anomalies
This repository holds the source code for the tools and programs used by the 
research team at Merit Network, Inc., in their development of the smart-grid 
anomaly detection tool known as ISGADA. For more information about the ISGADA 
project, you can read [our paper](isgada_paper.pdf).

Fellow researchers are free to use the tools provided to test their own data 
and to check our work. We only ask that you follow the guidelines of our 
[license](LICENSE.md).

## Setup
These tools are intended to run on a Raspberry Pi (model 2B or newer) running 
Raspbian Jessie. For instructions on how to set up a Raspberry Pi from scratch, 
see this document (TODO). Once the Pi has been set up, you can initialize all 
python dependencies and install the Razberry software on the Pi by running the 
setup.sh script:

`sudo ./setup.sh`

In order to collect data using Razberry, you will also need to purchase a few 
Z-Wave sensors and include them in your network; instructions on how to do this 
can be found on the
[Razberry website](http://razberry.z-wave.me/index.php?id=5).

## Usage
There are two main analysis tools in this repository:
[sequentialBLR.py](sequentialBLR.py) and [runCSV.py](runCSV.py).
The sequentialBLR.py program collects data from a network of sensors
and runs real-time analysis, and runCSV.py runs the same analysis
on saved data in CSV format.

### The Algorithm
The following is a brief description of the algorithm that is used in
the analysis:

Periodically (for example, once every minute), the program gathers data
about the house such as temperature, humidity, etc. as well as the total
power used during that minute. After a certain amount of data has been
collected (the "training window," for example 1000 measurements), the
program trains a prediction model based on the previous data. Then, using
this model, it tries to predict what the power measurement should be
based on incoming data. An anomaly is detectd when the actual power usage
and the predicted usage differ by some statistically significant margin.
The model is then retrained periodically with new data (the "training 
period").

### Running the Scripts
#### sequentialBLR.py

This program collects data from a network of Z-Wave sensors using the
[Razberry software by Z-Way](http://razberry.z-wave.me/index.php?id=1).
Once you have installed the software and included your sensors in the
network, you can begin to collect data by running the following on the Pi:

`./sequentialBLR.py localhost -o`

The -o flag tells the program to only collect data and not to run the
analysis. This will automatically save the data in a CSV file for
later use.
You can also run this command remotely from any computer on the same
network by replacing "localhost" with the IP or hostname of the Pi.

If you are running version 2.0.0 or newer of the Z-Way Server software,
you may require authentication to access the server. You can do this
by running the following command:

`sequentialBLR.py localhost -u my_username -p my_password`

**NOTE: In order to run the analysis, the "get_power" function in 
[sequentialBLR.py](sequentialBLR.py) must be implemented!** There is 
currently no standardized way of measuring this power data, so we 
leave it up to the user to fill this in.

To run the full analysis, you can run the following command:

`./sequentialBLR.py localhost -f <SETTINGS_FILE>`

Here, SETTINGS_FILE is a JSON-formatted file containing some important
analysis paramters:

* `granularity` - frequency with which data is collected, in seconds (recommended between 15 and 120)
* `training_window` - amount of data used to train on (time = training_window * granularity)
* `training_interval` - number of samples between training sessions (time = training_period * granularity)
* `auto_regression` - number of auto-regressive features (past power values)
* `ema_alpha` - hyper-parameter beteen 0.0 and 1.0, performs a moving average on samples
* `severity_omega` - hyper-parameter between 0.0 and 1.0, moving average of z-scores
* `severity_lambda` - hyper-parameter, threshold of z-scores which indicates an anomaly

To ignore these parameters and use the default values, the -f flag
can be omitted. Once the analysis starts, it will continue to collect
data until it has enough to train the model, at which point it will
start making predictions. These predictions, as well as the target
values and an anomaly alert flag (boolean) is saved to a separate CSV
file.

#### runCSV.py
Similar to sequentialBLR.py, this program runs the analysis software
except on previously collected data. This allows the user to try
different combinations of hyperparameters or modify the data in other
ways. It can be run as follows:

`./runCSV.py <INFILE> <OUTFILE> -f <SETTINGS_FILE>`

Here, INFILE is the name of the input CSV file, and OUTFILE is the
name of the results file that will be created. The settings are 
handled in the same way as sequentialBLR.py, except that the 
granularity parameter is ignored.

#### grapher.py
Once you have collected data and run your analysis, you can visualize the 
results using the grapher program. 
To start it, simply run as: `./grapher.py`

The grapher has a graphical interface that allows you to choose which results
file to plot as well as change some of the characteristics:

* **Start Date and End Date** - change the range of data being displayed
* **Smooth data (minutes)** - smooth out noisy data to help reveal overall trend
* **Show anomalies (minutes)** - show regions where alerts were raised by the algorithm
* **Update Graph** - apply changes and update the plot

You can use the toolbar to navigate, and clicking the Save button (floppy disk
icon) will allow you to save your plot as an image file.

## Contributing
This repository is currently maintained by the research team at Merit Network, Inc. 
If you would like to contribute to the project, please submit a pull request
and we will review your additions accordingly.

