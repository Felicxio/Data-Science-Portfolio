import urllib.request
import os

def download_database():
    """
    Downloads the NorthWind Database

    """
    url = "https://raw.githubusercontent.com/jpwhite3/northwind-SQLite3/main/dist/northwind.db"
    destination = "data/raw/northwind.db"

    if os.path.exists(destination):
        print(f"Database already exists in: {destination}")
        return
    
    print("Downloading the Northwind Database...")

    try:
        urllib.request.urlretrieve(url, destination)
        print(f'Download Concluded! Saved in: {destination}')

        #verifies the file size
        file_size = os.path.getsize(destination) / (1024 * 1024) #in MB
        print(f"File size : {file_size:.2f} MB")
    except Exception as e:
        print(f"Error in downloading : {e}")
if __name__ == "__main__":
    download_database()