from pathlib import Path

def search_json_files(dir_path: str | Path):
    if isinstance(dir_path, str):
        dir_path = Path(dir_path)

    return list(dir_path.rglob("*.json"))


# def main():
#     dir_path = "data"

#     print(search_json_files(dir_path))

# if __name__ == "__main__":
#     main()