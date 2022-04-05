from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class Day:
    year: int
    month: int
    day: int
    hour: int = 0
    minute: int = 0
    second: int = 0

    @property
    def date(self) -> str:
        return datetime(
            self.year, self.month, self.day, self.hour, self.minute, self.second
        ).date()

    @property
    def seconds_since_unix_epoch(self) -> int:
        return int(
            datetime(
                self.year,
                self.month,
                self.day,
                self.hour,
                self.minute,
                self.second,
                tzinfo=timezone.utc,
            ).timestamp()
        )
