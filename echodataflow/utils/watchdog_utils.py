import os
import time
import yaml
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from prefect import flow

class FileHandler(FileSystemEventHandler):
    def __init__(self):        
        self.previous_file = None

    def on_created(self, event):
        if event.is_directory:
            return
        if self.previous_file:
            self.process_previous_file(self.previous_file)
        self.previous_file = event.src_path

    def on_modified(self, event):
        if event.is_directory:
            return        

    def process_previous_file(self, file_path):        
        process_file.with_options()(file_path)
        


def main():
    path_to_watch = './temp'

    event_handler = FileHandler()
    observer = Observer()
    observer.schedule(event_handler, path=path_to_watch, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__": 
    main()