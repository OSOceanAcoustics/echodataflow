import http.client
import json
from typing import Any, Dict

def send_request(method : str, url : str, headers : Dict[str, Any] = {}, payload : Any = None):
    data = None
    try:
        conn = http.client.HTTPConnection("127.0.0.1", 4200)
        if method == "POST":
            headers.update({
            'Content-Type': 'application/json'
            })
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
    data : str = None
    id_value : str = None

    payload = json.dumps({
    "sort": "END_TIME_DESC",
    "limit": 1,
    "offset": 0,
    "flows": {
        "operator": "and_",
        "name": {
        "any_": [
            name
        ]
        }
    }
    })
    
    data = send_request(method="POST", payload=payload, url="/api/ui/flow_runs/history")

    if data is not None:
        parsed_data = json.loads(data)

        # Access the 'id' value from the first dictionary in the list
        if parsed_data and isinstance(parsed_data, list):
            first_dict = parsed_data[0]
            id_value = first_dict.get('id')
        
    return id_value
    

def get_last_run_history(name: str, type : str = "FLOW"):
    id = get_last_flow_run(name=name)
    run_history : str = None
    if id is not None:
        run_history = send_request(method="GET", url="/api/flow_runs/"+id+"/graph")
        run_json = json.loads(run_history)
        if run_json and not isinstance(run_json, str):
            return run_json
    return run_history
