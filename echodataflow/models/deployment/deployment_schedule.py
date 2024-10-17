from abc import ABC
from datetime import datetime
from pydantic import BaseModel, Field, field_validator

class DeploymentSchedule(BaseModel):
    anchor_date: datetime = Field(default_factory=datetime.now, description="The start date from which the schedule will compute its intervals in format DYYYYMMDD-THHMMSS.")
    interval_mins: int = Field(..., gt=0, description="The number of minutes between each scheduled run.")

    @field_validator('anchor_date', mode='before')
    def validate_and_convert_anchor_date(cls, v):
        # Check if the value is already a datetime object
        if isinstance(v, datetime):
            return v
        
        # Expecting anchor_date in the format 'DYYYYMMDD-THHMMSS'
        if not isinstance(v, str) or not v.startswith("D"):
            raise ValueError("anchor_date must be a string in format 'DYYYYMMDD-THHMMSS'")
        
        # Remove the leading 'D' and split the date and time parts
        try:
            date_part, time_part = v[1:].split("-T")
            # Convert to datetime object
            formatted_date = datetime.strptime(f"{date_part} {time_part}", "%Y%m%d %H%M%S")
            return formatted_date
        except ValueError as e:
            raise ValueError(f"Invalid anchor_date format: {v}. Error: {e}")