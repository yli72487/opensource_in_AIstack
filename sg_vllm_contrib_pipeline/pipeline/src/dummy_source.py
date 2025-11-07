import os
import pandas as pd

def write_dummy(out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    df = pd.DataFrame({
        "hello": ["world", "pipeline"],
        "value": [1, 2]
    })
    df.to_csv(os.path.join(out_dir, "dummy_output.csv"), index=False)
