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
    return conn, cur


def find_agent(cur, id):
    cur.execute("""
        SELECT name, email, phone
          FROM agents
         WHERE agentId = ?
    """, (id,))
    row = cur.fetchone()

    return row


def find_office(cur, id):
    cur.execute("""
        SELECT name, email, phone
          FROM offices
         WHERE officeId = ?
    """, (id,))
    row = cur.fetchone()

    return row


def main():
    conn, cur = init_db()
    PAGE = 0
    API_URL = f"https://cozying.ai/cozying-api/v1/home/list?currentPage={PAGE}&homesPerGroup=200&propertyStatus[]=active&sorted=newest&minPrice=0&maxPrice=0&minBeds=0&minBaths=0&hasOpenHouses=false&hasVirtualTour=false&type=rent"

    results = []
    response = requests.get(API_URL)
    data = response.json()
    homes = data.get("homes", [])

    while homes:
        print("[INFO] Scraping ", API_URL)
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
                "property_and_building_type": home.get("propertyType", ""),
            }

            # agent info
            agentId = home.get("agentId", "")
            officeId = home.get("officeId", "")
            agentRow = find_agent(cur, agentId)
            officeRow = find_office(cur, officeId)

            # Is the agent in our database
            if agentRow:
                name, email, number = agentRow
                rec.update({
                    "listing_provided_agent_name":   name,
                    "listing_provided_agent_email":  email,
                    "listing_provided_agent_number": number,
                })
            else:
                resp = requests.get(rec["link"])
                soup = bs(resp.text, "html.parser")
                agent_div = soup.find("div", class_="listing-information__agent")
                if agent_div:
                    for li in agent_div.select("ul li"):
                        text = li.get_text(strip=True).lstrip("• ").split(":", 1)
                        if len(text) == 2:
                            label, value = text
                            label = label.strip().lower()
                            value = value.strip()
                            if label == "name":
                                rec["listing_provided_agent_name"] = value
                            elif label == "email":
                                rec["listing_provided_agent_email"] = value
                            elif label == "phone":
                                rec["listing_provided_agent_number"] = value
                        
                    cur.execute("""
                    INSERT OR IGNORE INTO agents(agentId,name,email,phone)
                    VALUES(?,?,?,?)
                    """, (
                        agentId,
                        rec["listing_provided_agent_name"],
                        rec["listing_provided_agent_email"],
                        rec["listing_provided_agent_number"],
                    ))
                    conn.commit()

            # is that office in our database
            if officeRow:
                name, email, number = officeRow
                rec.update({
                    "listing_provided_office_name":   name,
                    "listing_provided_office_email":  email,
                    "listing_provided_office_number": number,
                })
            else:
                resp = requests.get(rec["link"])
                soup = bs(resp.text, "html.parser")
                office_div = soup.find("div", class_="listing-information__office")
                if office_div:
                    for li in office_div.select("ul li"):
                        text = li.get_text(strip=True).lstrip("• ").split(":", 1)
                        if len(text) == 2:
                            label, value = text
                            label = label.strip().lower()
                            value = value.strip()
                            if label == "name":
                                rec["listing_provided_office_name"] = value
                            elif label == "email":
                                rec["listing_provided_office_email"] = value
                            elif label == "phone":
                                rec["listing_provided_office_number"] = value
                    cur.execute("""
                    INSERT OR IGNORE INTO offices(officeId,name,email,phone)
                    VALUES(?,?,?,?)
                    """, (
                        officeId,
                        rec["listing_provided_office_name"],
                        rec["listing_provided_office_email"],
                        rec["listing_provided_office_number"],
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
            print(rec)

        PAGE += 1
        API_URL = f"https://cozying.ai/cozying-api/v1/home/list?currentPage={PAGE}&homesPerGroup=200&propertyStatus[]=active&sorted=newest&minPrice=0&maxPrice=0&minBeds=0&minBaths=0&hasOpenHouses=false&hasVirtualTour=false&type=rent"
        response = requests.get(API_URL)
        data = response.json()
        homes = data.get("homes", [])

    df = pd.DataFrame(results)
    df.to_excel("homes-rent.xlsx", index=False)
    print(len(results), " scraped")

    cur.close()
    conn.close()


if __name__ == "__main__":
    start = time.perf_counter()
    main()
    end = time.perf_counter()
    print("Code Ran For:", timedelta(seconds=(end - start)))