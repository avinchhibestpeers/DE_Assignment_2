from pathlib import Path
import zipfile

def unzip_csv_file(zip_file_path: str|Path) -> Path:
    """
    - Unzip and extract csv file. 
    - Returns csv file path.
    """
    if isinstance(zip_file_path, str):
        zip_file_path = Path(zip_file_path)
    csv_file_path = zip_file_path.with_suffix(".csv")
    with zipfile.ZipFile(zip_file_path) as zip_file, open(csv_file_path, "wb") as csv_file:
        for filename in zip_file.namelist():
            if not filename.startswith("_") and filename.endswith(".csv"):
                csv_file.write(zip_file.read(filename))
                break
    
    return csv_file_path
            

def unzip_files(source_dir: Path|str) -> list[Path]:
    """
    Given a directory, extract csv file from all zip files.
    """
    if isinstance(source_dir, str):
        source_dir = Path(source_dir)
        
    csv_file_paths = []

    for zip_file_path in source_dir.iterdir():
        csv_file_path = unzip_csv_file(zip_file_path)
        csv_file_paths.append(csv_file_path)    

    return csv_file_paths
    
