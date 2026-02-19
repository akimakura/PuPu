from logging import Filter, LogRecord
from typing import List


class Blacklist(Filter):
    def __init__(self, blacklist: List[str]) -> None:
        super(Blacklist, self).__init__()
        self.blacklist = blacklist

    def filter(self, record: LogRecord) -> bool:
        message = record.getMessage()
        return all(message.find(word) == -1 for word in self.blacklist)
