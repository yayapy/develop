from shutil import copyfile
import time
import os
import sys



"""This fonction returns  the timestamped version of a string
"""

def PrintFileName(filename):
    timestr = time.strftime("%Y%m%d-%H%M%S")
    return filename+timestr
    
    
"""This script takes a tab and copy it to a location. The file will be named SwissChalet_ServiceArea_*Timestamp*
 """

filename='SwissChalet_ServiceArea_'
#timestamp version
filename2=PrintFileName(filename)

"""
------------------------------------------------------PARAMETERS------------------------------------------
"""


# Path of source workspace
"""Change the src variable to specify your workspace path"""
src=os.path.realpath('//Sv1w-gis-kp01//d$//GIS//sdmena//Recipe//SaveTab//Data//Swiss.wor')

"""Change the dst variable to specify the destination path for the tab file"""
# Path of destination file
dst=os.path.realpath('//Sv1w-gis-kp01//d$//GIS//sdmena//Recipe//SaveTab//Data//'+filename2+'.TAB')




    
    
"""
-----------------------------------------------------------MAIN---------------------------------------------
"""
# Open the workspace
do("run application \"{}\" mode current".format(src))
# Copy the service area table
do("Commit Table SwissChalet_ServiceAreas As \"{}\" TYPE NATIVE Charset \"WindowsLatin1\" Interactive".format(dst))


# Closing of MapInfoPro -> ending of script
while 1 :
    os.system("TASKKILL /F /IM mapinfopro.exe")
    time.sleep(10)
