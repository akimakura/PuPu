from enum import StrEnum


class ConnetionTypeEnum(StrEnum):
    LOAD_BALANCER = "LOAD_BALANCER"
    DATAGATE = "DATAGATE"
    NODE = "NODE"
