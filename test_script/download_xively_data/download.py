import datetime
import re
from subprocess import call
import sys 

USER = "nesl_test"
XIVELY_KEY = "NAOwnSMKvck-PWL9nKOSdMoQ-0WSAKxaV2dDRjhwYTRrdz0g"

FEEDS = { 
  'NESL_TempSensor': [ 'Temperature' ],
  'NESL_SmartSwitch': [ 'Energy', 'Power' ],
  'NESL_Veris': [ 'Current[0-20]', 'Power[0-20]', 'PowerFactor[0-20]' ],
  'NESL_LightSensor': [ 'Light' ],
  'NESL_MotionSensor': [ 'Motion' ],
  'CG_MotionSensor': [ 'Motion' ],
  'Solder_MotionSensor': [ 'Motion' ],
  'NESL_DoorSensor': [ 'Door' ],
  'NESL_Raritan': [ 'ActivePower[1-8]', 'ApparentPower[1-8]', 'Current[1-8]', 'PowerFactor[1-8]', 'Voltage[1-8]' ],
  'Conf_DoorSensor': [ 'Door' ],
  'NESL_Eaton': [ 'Current[A-C]', 'Power[A-C]', 'PowerFactor[A-C]', 'VARs[A-C]', 'VAs[A-C]', 'Voltage[A-C]N' ],
  'BreakBeam': [ 'Motion' ],
  'NESL_Occupancy': [ 'Occupancy_count' ] 
}

FROM_DATETIME = datetime.datetime.combine(datetime.date(2013,3,1), datetime.time(0,0))
TO_DATETIME = datetime.datetime.combine(datetime.date(2013,11,1), datetime.time(0,0))

AWEEK = datetime.timedelta(days=7)
  
DATETIME_FORMAT = "%Y-%m-%dT%H:%M"
DATETIME_FORMAT_FOR_FILE = "%Y-%m-%dT%H-%M"

SERVER_URL = "http://128.97.93.30:9005/xively/download"



def download(feed_name, datastream_name):

  start_ts = FROM_DATETIME

  while True:

    end_ts = start_ts + AWEEK

    start_time_str = start_ts.strftime(DATETIME_FORMAT)
    end_time_str = end_ts.strftime(DATETIME_FORMAT)

    url = SERVER_URL + "?user=" + USER + "&key=" + XIVELY_KEY + "&feed=" + feed_name + "&datastream=" + datastream_name + "&start=" + start_time_str + "&end=" + end_time_str

    start_time_file_str = start_ts.strftime(DATETIME_FORMAT_FOR_FILE)
    end_time_file_str = end_ts.strftime(DATETIME_FORMAT_FOR_FILE)

    filename = feed_name + '__' + datastream_name + '__' + start_time_file_str + '_' + end_time_file_str + '.csv'

    print 'Processing:', filename, '...'

    call(['curl', '-o', filename, '--request', 'GET', url])

    start_ts += AWEEK

    if start_ts > TO_DATETIME:
      break



def main():

  for feed_name, datastream_list in FEEDS.iteritems():
    for datastream_name in datastream_list:
      m = re.search('\[[A-Z0-9-]+\]', datastream_name)
      if m == None:
        #download(feed_name, datastream_name)
        pass
      else:
        pattern = m.group(0)
        ranges = pattern.replace('[','').replace(']','').split('-')

        try:
          ranges[0] = int(ranges[0])
          ranges[1] = int(ranges[1])
          isIntRange = True
        except ValueError:
          ranges[0] = ord(ranges[0])
          ranges[1] = ord(ranges[1])
          isIntRange = False
       
        for num in xrange(ranges[0], ranges[1]+1):
          if isIntRange:
            str_num = str(num)
          else:
            str_num = chr(num)
         
          new_datastream_name = datastream_name.replace(pattern, str_num)

          download(feed_name, new_datastream_name)



if __name__ == "__main__":
  main();

