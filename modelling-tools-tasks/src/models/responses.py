from pydantic import BaseModel, Field


class TaskResponse(BaseModel):
    msg: str = Field(default="Task created.")
    task_id: str = Field(serialization_alias="taskId")


class TaskResultResponse(BaseModel):
    task_id: str = Field(serialization_alias="taskId")
    error: str | None = Field(default=None)
    log: str | None = Field(default=None)
    execution_time: float | None = Field(default=None, serialization_alias="executionTime")
    return_value: str | None = Field(default=None, serialization_alias="returnValue")
    labels: str | None = Field(default=None)
