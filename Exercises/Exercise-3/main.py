import os

from src.fetch import download_and_print_s3_file, extract_file_uris

def main():
    base_url = "https://data.commoncrawl.org/"
    uri_container_file_url = os.path.join(base_url, "crawl-data/CC-MAIN-2022-05/wet.paths.gz")

    # pipeline
    uris = extract_file_uris(uri_container_file_url)
    print("Uris: ", uris[:5])
    download_and_print_s3_file(os.path.join(base_url,uris[0]), "downloads")


if __name__ == "__main__":
    main()
