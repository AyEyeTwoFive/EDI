# -*- coding: utf-8 -*-
"""
Created on Tue Oct 27 13:17:10 2020
Wrapper to check for new EDI file and run process script
@author: asef islam, aislam@qralgroup.com
"""

import time 
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import EDI_to_csv_v3

class ExampleHandler(FileSystemEventHandler):
    def on_created(self, event): # when file is created
        print("Got event for file %s" % event.src_path) 
        EDI_to_csv_v3.process_edi(event.src_path)
        

observer = Observer()
event_handler = ExampleHandler() # create event handler
# set observer to use created handler in directory
observer.schedule(event_handler, path= "C:/Users/islam/OneDrive - Qral Group/EDI/Input")
observer.start()

# sleep until keyboard interrupt, then stop + rejoin the observer
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()

observer.join()