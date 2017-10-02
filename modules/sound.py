import subprocess

# Get the max volume from the microphone
# sample_time is in seconds
def get_sound(sample_time=1):
    for x in xrange(10): 
        #command = "ssh cherry '/usr/bin/arecord -D plughw:1,0 -d " + str(sample_time) + " -f S16_LE | /usr/bin/sox -t .wav - -n stat'"
        command = "/usr/bin/arecord -D plughw:1,0 -d " + str(sample_time) + " -f S16_LE | /usr/bin/sox -t .wav - -n stat"
    
        p = subprocess.Popen(command, bufsize=1, shell=True,  stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        for line in p.stdout:
            if "Maximum amplitude" in line:
                return float(line.split()[-1])
        print "Audio Sensor Timeout. Attempting to reconnect"
        time.sleep(1)
     
    print "Audio Sensor Failed"

