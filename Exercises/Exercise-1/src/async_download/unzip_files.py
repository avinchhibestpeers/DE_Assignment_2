import zipfile 
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor


def unzip_file(zip_file_path: str|Path, des_path: Path) -> list[Path]:
    """
    Extract all files from zip file.
    """
    if isinstance(zip_file_path, str):
        zip_file_path = Path(zip_file_path)
    file_paths = []
    with zipfile.ZipFile(zip_file_path) as handler:
        # extract files
        for filename in handler.namelist():
            if not filename.startswith("_"):
                file_path = handler.extract(filename, des_path)
                file_paths.append(Path(file_path))

        # delete zip file
        zip_file_path.unlink()
        
    return file_paths
            

def sync_unzip_file(params: tuple):
    return unzip_file(*params)

def unzip_all_files(zip_file_paths: list[str|Path], des_path: str|Path) -> list[Path]:
    """
    parallel processing here.
    """
    if isinstance(des_path, str):
        des_path = Path(des_path)

    with ProcessPoolExecutor() as executor:
        result = list(
            executor.map(
                sync_unzip_file,
                [(path, des_path) for path in zip_file_paths]
            )
        )
    return result



# if __name__ == "__main__":
#     import os
#     zip_files = [Path("downloads") / f for f in os.listdir("downloads") if f.endswith(".zip")]

#     unzip_all_files(zip_files, Path("downloads"))