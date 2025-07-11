import requests
from bs4 import BeautifulSoup as bs
import sqlite3
import pandas as pd
import time
from datetime import timedelta
from main import SCRAPE_PARCEL


def init_db():
    conn = sqlite3.connect("agents_and_offices.db")
    cur = conn.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS agents (
        agentId TEXT PRIMARY KEY,
        name    TEXT,
        email   TEXT,
        phone   TEXT
      )
    """)
    cur.execute("""
      CREATE TABLE IF NOT EXISTS offices (
        officeId TEXT PRIMARY KEY,
        name      TEXT,
        email     TEXT,
        phone     TEXT
      )
    """)
    conn.commit()
    return conn, cur


def main():
    conn, cur = init_db()
    PAGE = 0
    API_URL = f"https://cozying.ai/cozying-api/v1/home/list?currentPage={PAGE}&homesPerGroup=200&propertyStatus[]=active&sorted=newest&minPrice=0&maxPrice=0&minBeds=0&minBaths=0&hasOpenHouses=false&hasVirtualTour=false&type=sale"

    results = []
    response = requests.get(API_URL)
    data = response.json()
    homes = data.get("homes", [])

    while homes:
        print("[INFO] Scraping ", API_URL)
        print("homes len: ", len(homes))
        for home in homes:
            full_addr = home.get("fullAddress", "")
            street, *rest = full_addr.split(",")
            rest = ", ".join(rest).strip()
            zipcode = rest.split()[-1] if rest else ""

            rec = {
                "link":           "https://cozying.ai" + home.get("url", ""),
                "street":         street.strip(),
                "zip":            zipcode,
                "price":          home.get("price", 0),
                "beds":           home.get("beds") or 0,
                "baths":          home.get("baths") or 0,
                "sf1":            home.get("size", 0),
                "sf2":            int(home.get("lotSizeSqft") or 0),
                "year":           home.get("yearBuilt") or 0,
                "property_and_building_type": home.get("cozyingPropertyType", ""),
            }

            # agent info
            agent = home.get("agent", {})
            agentId = agent.get("agentId", "")
            rec.update({
                "listing_provided_agent_name":   agent.get("agentName", ""),
                "listing_provided_agent_email":  agent.get("agentEmail", ""),
                "listing_provided_agent_number": agent.get("agentPhone", ""),
            })

            # office info
            office = home.get("agentOffice", {})
            officeId = office.get("officeId", "")
            rec.update({
                "listing_provided_office_name":   office.get("officeName", ""),
                "listing_provided_office_email":  office.get("officeEmail", ""),
                "listing_provided_office_number": office.get("officePhone", ""),
            })

            # Save Agent to Database
            cur.execute("""
              INSERT OR IGNORE INTO agents(agentId,name,email,phone)
              VALUES(?,?,?,?)
            """, (
              agentId,
              agent.get("agentName",""),
              agent.get("agentEmail",""),
              agent.get("agentPhone","")
            ))

            # Save Office to Database
            cur.execute("""
              INSERT OR IGNORE INTO offices(officeId,name,email,phone)
              VALUES(?,?,?,?)
            """, (
              officeId,
              office.get("officeName",""),
              office.get("officeEmail",""),
              office.get("officePhone","")
            ))
            conn.commit()

            # parcel number
            if SCRAPE_PARCEL:
                try:
                    resp = requests.get(rec["link"])
                    resp.raise_for_status()
                    soup = bs(resp.text, "html.parser")
                    details_label = soup.find(
                        "span", class_="item-title", string="Details"
                    )

                    parcel_number = None
                    if details_label:
                        # 2) its next sibling <ul> contains the <li> items
                        ul = details_label.find_next_sibling("ul")
                        if ul:
                            for li in ul.find_all("li"):
                                text = li.get_text(strip=True)
                                if "Parcel Number:" in text:
                                    # 3) split off the number
                                    _, num = text.split(":", 1)
                                    parcel_number = num.strip()
                                    break

                    # store it on your record
                    rec["parcel_number"] = parcel_number

                except:
                    pass

            results.append(rec)
            print(len(results), " scraped")
            print(rec, "\n")

        PAGE += 1
        API_URL = f"https://cozying.ai/cozying-api/v1/home/list?currentPage={PAGE}&homesPerGroup=200&propertyStatus[]=active&sorted=newest&minPrice=0&maxPrice=0&minBeds=0&minBaths=0&hasOpenHouses=false&hasVirtualTour=false&type=sale"
        response = requests.get(API_URL)
        data = response.json()
        homes = data.get("homes", [])

    df = pd.DataFrame(results)
    df.to_excel("homes-sell.xlsx", index=False)
    print(len(results), " scraped")

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    start = time.perf_counter()
    main()
    end = time.perf_counter()
    print("Code Ran For:", timedelta(seconds=(end - start)))