import time
from datetime import timedelta

SCRAPE_PARCEL = False

def main():
    global SCRAPE_PARCEL
    scrape_parcel = input("Do you want to scrape parcel number? (Y/N)")
    if "y" in scrape_parcel.lower():
        SCRAPE_PARCEL = True
        print("Scraping parcel number too")
    elif "n" in scrape_parcel.lower():
        SCRAPE_PARCEL = False
        print("Will not scrape parcel number")

    import reqsell
    reqsell.main()
    import reqrent
    reqrent.main()

if __name__ == "__main__":
    start = time.perf_counter()
    main()
    end = time.perf_counter()
    print("Code Ran For:", timedelta(seconds=(end - start)))