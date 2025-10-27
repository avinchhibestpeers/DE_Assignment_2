from pathlib import Path
import csv
import json

def flatten_json(data: dict[list|dict|int|str]) -> dict[int|str]:
    """
    Recursively flatten out json data
    Example:
        input = {
            "a": [1,2],
            "b": {"c" : "34"},
            "d": 3
        }
        output = {
            "a_0": 1,
            "a_1": 2,
            "b_c": "34"
        }
    """
    csv_result = {}

    # recursively extract all columns
    def helper(key, value):
        # if value list
        if isinstance(value, list):
            for i,v in enumerate(value):
                helper(f"{key}_{i}", v)

        # if value dict
        elif isinstance(value ,dict):
            for k,v in value.items():
                helper(f"{key}_{k}", v)

        # if primitive type
        else:
            csv_result[key] = value

    for key, value in data.items():
        helper(key, value)
    
    return csv_result

def _json_to_csv(file_path: str|Path, des_dir: Path):
    """
    Read Json file convert it into csv file
    """
    if isinstance(file_path, str):
        file_path = Path(file_path)

    with file_path.open("r") as file:
        key_val_store = json.load(file)

    csv_result = flatten_json(key_val_store)
    
    # create csv file path
    file_path_json = des_dir / file_path.name
    file_path_csv = file_path_json.with_suffix(".csv")

    # write csv file
    with file_path_csv.open("w") as file:
        writer = csv.DictWriter(file, fieldnames=csv_result.keys())
        writer.writeheader()
        writer.writerow(csv_result)

    return file_path_csv



def json_to_csv(file_paths: list[str|Path], des_dir: str|Path) -> list[Path]:
    if isinstance(des_dir, str):
        des_dir = Path(des_dir)
    
    des_dir.mkdir(exist_ok=True)

    csv_file_paths = []
    
    for file_path in file_paths:
        try:
            csv_file_path = _json_to_csv(file_path, des_dir)
            csv_file_paths.append(csv_file_path)
        except Exception as e:
            print(f"ERROR: file_path: {file_path} \nerror: {e}")
    
    return csv_file_paths


# if __name__ == "__main__":
#     des_path = Path("downloads")
#     des_path.mkdir(exist_ok=True)
#     print(_json_to_csv("data/some_folder/other_folder/file-2.json", des_path))
