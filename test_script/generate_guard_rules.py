import datetime
from subprocess import call
import json

SENSORSAFE_USER = "nesl_owner"
SENSORSAFE_USER_PASSWD = "neslrocks!"
SENSORSAFE_URL = "https://128.97.93.251:8443/api"

DATETIME_FORMAT = "%Y-%m-%d %H:%M:%M"

START_TS = datetime.datetime.combine(datetime.date(2013,3,1), datetime.time(0,0))
TS_DELTA = datetime.timedelta(weeks=4)
TS_DELTA_DAY = datetime.timedelta(days=1)



def deleteAllRules():
  
  call(['curl', '-k', '--request', 'DELETE', 
      '--user', SENSORSAFE_USER+':'+SENSORSAFE_USER_PASSWD, SENSORSAFE_URL+'/rules'])
  print


  
def deleteAllMacros():
  
  call(['curl', '-k', '--request', 'DELETE', 
      '--user', SENSORSAFE_USER+':'+SENSORSAFE_USER_PASSWD, SENSORSAFE_URL+'/macros'])
  print


def addARule(rule):

  strRule = json.dumps(rule)

  call(['curl', '-k', '--request', 'POST', 
      '--header', 'Content-Type: application/json',
      '--data', strRule,
      '--user', SENSORSAFE_USER+':'+SENSORSAFE_USER_PASSWD, SENSORSAFE_URL+'/rules'])
  print



def addAMacro(macro):

  strMacro = json.dumps(macro)
  call(['curl', '-k', '--request', 'POST', 
      '--header', 'Content-Type: application/json',
      '--data', strMacro,
      '--user', SENSORSAFE_USER+':'+SENSORSAFE_USER_PASSWD, SENSORSAFE_URL+'/macros'])
  print



def generateTimestampRules(numRules):
  global START_TS, TS_DELTA, TS_DELTA_DAY

  for num in xrange(0,numRules):
    
    if num % 2 == 0:
      start_ts = START_TS + datetime.timedelta(days=num)
      rule = { 'targetStreams': [ 'NESL_Veris__Current0' ],
               'condition': 'timestamp BETWEEN "'
                  +start_ts.strftime(DATETIME_FORMAT)+'" AND "'
                  +(start_ts+TS_DELTA).strftime(DATETIME_FORMAT)+'"',
               'action': 'allow' }
    
    else:
      start_ts = START_TS + ( datetime.timedelta(weeks=1) * (num/2+1) )
      rule = { 'targetStreams': [ 'NESL_Veris__Current0' ],
               'condition': 'timestamp BETWEEN "'
                  +start_ts.strftime(DATETIME_FORMAT)+'" AND "'
                  +(start_ts+TS_DELTA_DAY).strftime(DATETIME_FORMAT)+'"',
               'action': 'deny' }
    
    addARule(rule)


  
def generateValueRules(numRules):

  for num in xrange(0,numRules):

    if num % 2 == 0:

      rule = { 'targetStreams': [ 'NESL_Veris__Current0' ],
               'condition': 'channel1 BETWEEN ' + str(num) + ' AND ' + str(num+50),
               'action': 'allow' }

    else:

      rule = { 'targetStreams': [ 'NESL_Veris__Current0' ],
               'condition': 'channel1 BETWEEN ' + str(num) + ' AND ' + str(num+0.0000001),
               'action': 'deny' }

    addARule(rule)



def generateMacroRules(numRules):

  for num in xrange(0,numRules):

    if num % 2 == 0:

      rule = { 'targetStreams': [ 'NESL_Veris__Current0' ],
               'condition': '$(WORK_TIME_' + str(num+1) + ')',
               'action': 'allow' }

    else:

      rule = { 'targetStreams': [ 'NESL_Veris__Current0' ],
               'condition': '$(WORK_TIME_' + str(num+1) + ')',
               'action': 'deny' }

    addARule(rule)



def generateMacros(numMacros):

  for num in xrange(0, numMacros):

    day = 7 + num
    month = 3
    if day > 30:
      month += day / 30
      day = day % 30 + 1

    macro = { 'name': 'WORK_TIME_' + str(num+1), 'value': '[ * * 9-18 ' + str(day) + ' ' + str(month) + ' * ]' }

    addAMacro(macro)
 


def generateMacroRules():
  numRules = 100
  deleteAllMacros()
  generateMacros(numRules)
  deleteAllRules()
  generateMacroRules(numRules)
  


def main():

  deleteAllRules()
  generateTimestampRules(4)
  #generateValueRules(100)
  
  #generateMacroRules()





if __name__ == '__main__':
  main()
