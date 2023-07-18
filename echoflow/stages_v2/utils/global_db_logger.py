from datetime import datetime
from echoflow.config.models.log_object import Log, Log_Data, Process

def init_global(init_log: Log):
    global log
    log = init_log

def add_new_process(process: Process, name: str):
    process.end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    if name not in log.data:
        log.data[name] = Log_Data(name=name)
    log.data[name].process_stack.append(process)

def get_log():
    global log
    if log is not None:
        return log
    else:
        return Log()
