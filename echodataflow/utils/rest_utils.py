"""
Module containing various utility functions for interacting with external systems.

This module provides functions for sending HTTP requests, retrieving flow run history,
and handling file systems and paths.

Functions:
    send_request(method, url, headers={}, payload=None):
        Send an HTTP request to a specified URL with optional headers and payload.

    get_last_flow_run(name, type="FLOW"):
        Get the ID of the last flow run with the specified name.

    get_last_run_history(name, type="FLOW"):
        Get the history of the last flow run with the specified name.

Author: Soham Butala
Email: sbutala@uw.edu
Date: August 22, 2023
"""
import http.client
import json
from typing import Any, Dict


def send_request(method : str, url : str, headers : Dict[str, Any] = {}, payload : Any = None):
    """
    Send an HTTP request to a specified URL with optional headers and payload.

    Parameters:
        method (str): The HTTP method, e.g., "GET" or "POST".
        url (str): The URL to send the request to.
        headers (Dict[str, Any]): Optional headers to include in the request.
        payload (Any): Optional payload data to send with the request.

    Returns:
        str: The response data as a string.

    Example:
        response = send_request(method="GET", url="https://example.com/api/data")
    """
    data = None
    try:
        conn = http.client.HTTPConnection("127.0.0.1", 4200)
        if method == "POST":
            headers.update({"Content-Type": "application/json"})
            conn.request(str(method), url, payload, headers)
        elif method == "GET":
            conn.request(method=str(method), url=url, headers=headers)

        res = conn.getresponse()
        data = res.read().decode("utf-8")
    except Exception as e:
        print(e)
    finally:
        conn.close()
    return data


def get_last_flow_run(name : str, type : str = "FLOW"):
    """
    Get the ID of the last flow run with the specified name.

    Parameters:
        name (str): The name of the flow.
        type (str): The type of flow (default: "FLOW").

    Returns:
        str: The ID of the last flow run, or None if no flow runs were found.

    Example:
        flow_name = "MyFlow"
        last_run_id = get_last_flow_run(name=flow_name)
    """
    data : str = None
    id_value : str = None

    payload = json.dumps(
        {
    "sort": "END_TIME_DESC",
    "limit": 1,
    "offset": 0,
            "flows": {"operator": "and_", "name": {"any_": [name]}},
        }
    )
    
    data = send_request(method="POST", payload=payload, url="/api/ui/flow_runs/history")

    if data is not None:
        parsed_data = json.loads(data)

        # Access the 'id' value from the first dictionary in the list
        if parsed_data and isinstance(parsed_data, list):
            first_dict = parsed_data[0]
            id_value = first_dict.get("id")
        
    return id_value
    

def get_last_run_history(name: str, type : str = "FLOW"):
    """
    Get the history of the last flow run with the specified name.

    Parameters:
        name (str): The name of the flow.
        type (str): The type of flow (default: "FLOW").

    Returns:
        Any: The run history data, or None if no history was found.

    Example:
        flow_name = "MyFlow"
        last_run_history = get_last_run_history(name=flow_name)
    """
    id = get_last_flow_run(name=name)
    run_history : str = None
    if id is not None:
        run_history = send_request(method="GET", url="/api/flow_runs/"+id+"/graph")
        run_json = json.loads(run_history)
        if run_json and not isinstance(run_json, str):
            return run_json
    return run_history
