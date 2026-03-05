import os
import pandas as pd
from datetime import datetime


class ParquetWriter:

    def __init__(self, base_dir="data/orders", batch_size=10):

        self.base_dir = base_dir
        self.batch_size = batch_size
        self.buffer = []

    def add_event(self, event):

        self.buffer.append(event)

        if len(self.buffer) >= self.batch_size:
            self.flush()

    def flush(self):

        if not self.buffer:
            return

        df = pd.DataFrame(self.buffer)

        # Extract timestamp for partitioning
        ts = datetime.fromisoformat(df["timestamp"].iloc[0])

        year = ts.strftime("%Y")
        month = ts.strftime("%m")
        day = ts.strftime("%d")

        partition_path = os.path.join(
            self.base_dir,
            f"year={year}",
            f"month={month}",
            f"day={day}"
        )

        os.makedirs(partition_path, exist_ok=True)

        file_name = f"orders_{pd.Timestamp.utcnow().value}.parquet"

        file_path = os.path.join(partition_path, file_name)

        df.to_parquet(file_path, engine="pyarrow")

        print(f"Written {len(self.buffer)} records to {file_path}")

        self.buffer.clear()