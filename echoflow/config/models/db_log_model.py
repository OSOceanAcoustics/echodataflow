

from datetime import datetime
from typing import Dict, List, Optional
from pydantic import BaseModel


class Process(BaseModel):
    name: str
    start_time: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    end_time: Optional[str]
    status: bool = False
    error: Optional[str]


class Log_Data(BaseModel):
    name: str
    process_stack: Optional[List[Process]] = []


class DB_Log(BaseModel):
    run_id: Optional[int]
    start_time: Optional[str] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
    end_time: Optional[str]
    data: Optional[Dict[str, Log_Data]] = {}
    status: str = True
    error: Optional[str]






    

