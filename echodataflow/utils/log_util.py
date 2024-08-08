

import asyncio
import logging
import os
import traceback
from typing import Any, Coroutine

from distributed import get_client

from echodataflow.aspects.singleton_echodataflow import Singleton_Echodataflow
from echodataflow.models.datastore import EchodataflowLogs
from kafka import KafkaProducer
import json


def log(
    stream_name: str = "echodataflow", msg:Any="", use_dask:bool=False, eflogging: EchodataflowLogs = None,
    error: Exception = None, level= logging.DEBUG
):
    """
    Logs a message to the specified stream and optionally forwards it to Kafka.

    This function handles logging within Echodataflow, offering flexibility to log through
    Dask's distributed logging system, Echodataflow's singleton logger, or directly to a Kafka topic if configured.

    Parameters:
    - stream_name (str): The name of the stream where the message will be logged. Defaults to 'echodataflow'.
    - msg (Any): The message to log. This should be a dictionary containing at least 'mod_name', 'func_name', and 'msg'.
                 The 'mod_name' will be modified to just the basename of the module path.
    - use_dask (bool): Determines whether to use Dask's logging mechanism. Defaults to False.
    - eflogging (EchodataflowLogs): An EchodataflowLogs object containing Kafka configuration. If provided and configured,
                                messages will also be sent to a Kafka topic.

    The function prints the message to the console and, based on the configuration, logs it using the appropriate method.
    If Kafka logging is enabled and configured, it attempts to send the message to the specified Kafka topic.

    Exceptions within Kafka logging are caught and reported to the console, but do not interrupt execution.

    Note: The function assumes that if `use_dask` is True, a Dask distributed client is available and configured.
    """
    if msg.get("mod_name"):
        msg["mod_name"] = os.path.basename(msg["mod_name"])

    if error:
        msg["msg"] = traceback.format_exc()
    
    if use_dask:
        get_client().log_event(stream_name, msg=msg)
    else:
        if Singleton_Echodataflow.get_instance():
            Singleton_Echodataflow.get_instance().log(
                msg=msg["msg"],
                extra={"mod_name": msg["mod_name"], "func_name": msg["func_name"]},
                        level=level,
                    )
            
    if eflogging:
        if isinstance(eflogging, dict):
            eflogging = EchodataflowLogs(**eflogging)
        if eflogging.kafka:
            if eflogging.kafka.topic and len(eflogging.kafka.servers) != 0:
                producer = None
                try:
                    producer = KafkaProducer(
                        bootstrap_servers=eflogging.kafka.servers,
                        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    )
                    producer.send(eflogging.kafka.topic, msg)
                except Exception as e:
                    print("Failed logging to Kafka due to", e)
                finally:
                    if producer:
                        producer.flush()
                        producer.close()
        
    print(f"{msg.get('mod_name')}  {msg.get('func_name')} : {msg.get('msg')}")

# event is a tuple of (timestamp, msg)
def log_event(event):
    gea = Singleton_Echodataflow.get_instance()
    if gea:
        gea.log(
            msg=event[1]["msg"],
            extra={"mod_name": event[1]["mod_name"], "func_name": event[1]["func_name"]},
            level=logging.DEBUG,
        )
