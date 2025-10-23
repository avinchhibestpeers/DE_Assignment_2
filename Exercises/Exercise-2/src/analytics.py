import pandas as pd
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

def max_hourly_bulb_temp(file_path: str|Path) -> float:
    """
    Returns max value of column `HourlyDryBulbTemperature`
    """
    df = pd.read_csv(file_path)

    temps = df.HourlyDryBulbTemperature.dropna()

    # filter floating values
    temps = temps[temps.astype('str').str.match(r"^-?\d+\.?\d+$")]
    temps = temps.astype(float)
    
    return temps.max()

def max_hourly_bulb_temp_all(file_paths: list[str|Path]) -> list[float]:
    """
    From all csv files extract max value of column HourlyDryBulbTemperature
    """
    with ProcessPoolExecutor() as executor:
        futures = []
        for file_path in file_paths:
            future = executor.submit(max_hourly_bulb_temp, file_path)
            futures.append(future)
    
    values = []
    for future in futures:
        try:
            val = future.result()
            if not pd.isna(val):
                values.append(val)
        except Exception as e:
            print(f"EXCEPTION: {e}")
    
    return values