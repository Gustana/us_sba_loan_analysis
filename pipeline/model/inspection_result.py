from pydantic import BaseModel
from typing import Optional, Union

class InspectionResultModel(BaseModel):
    """
    Inspection result model
    """

    needs_to_verify: bool
    actual_value: Union[str, int, float]
    verify_reason: Optional[str] = None
    value_to_assign: Union[str, int, None] = None