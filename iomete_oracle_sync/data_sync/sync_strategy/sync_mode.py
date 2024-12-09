from datetime import datetime, timezone


class SyncMode:
  pass


class FullLoad(SyncMode):
  def __str__(self):
    return "full_load"


class IncrementalLoad(SyncMode):
  def __init__(self, primary_key: str, partition_column: str, start_date: str | None = None,
               end_date: str | None = None):
    self.primary_key = primary_key
    self.partition_column = partition_column
    self.start_date = start_date
    self.end_date = end_date or datetime.now(timezone.utc).date().isoformat()

  def __str__(self):
    return (
      f"incremental_load("
      f"primary_key: '{self.primary_key}', "
      f"partition_column: '{self.partition_column}', "
      f"start_date: '{self.start_date}', "
      f"end_date: '{self.end_date}'"
    )
