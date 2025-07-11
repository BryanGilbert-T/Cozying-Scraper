import asyncio
import pandas as pd
import time
import sqlite3
from playwright.async_api import async_playwright

URL = "https://cozying.ai/los-angeles-ca/rent?page=1"
MAX_CONCURRENCY = 10

conn = None
cur = None

def insert_sql(res):
    global conn, cur
    insert_sql = """
        INSERT OR REPLACE INTO listings (
            link, street, zip, price, beds, baths, sf1, sf2, year,
            property_and_building_type,
            listing_provided_agent_name, listing_provided_agent_email, listing_provided_agent_number,
            listing_provided_office_name, listing_provided_office_email, listing_provided_office_number,
            parcel_number
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    cur.execute(insert_sql, (
        res["link"],
        res["street"],
        res["zip"],
        res["price"],
        res["beds"],
        res["baths"],
        res["sf1"],
        res["sf2"],
        res["year"],
        res["property_and_building_type"],
        res["listing_provided_agent_name"],
        res["listing_provided_agent_email"],
        res["listing_provided_agent_number"],
        res["listing_provided_office_name"],
        res["listing_provided_office_email"],
        res["listing_provided_office_number"],
        res["parcel_number"],
    ))
    conn.commit()


def init_db():
    global conn, cur
    conn = sqlite3.connect("properties-rent.db")
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS listings (
        link TEXT PRIMARY KEY,
        street TEXT,
        zip TEXT,
        price INTEGER,
        beds INTEGER,
        baths INTEGER,
        sf1 INTEGER,
        sf2 INTEGER,
        year INTEGER,
        property_and_building_type TEXT,
        listing_provided_agent_name TEXT,
        listing_provided_agent_email TEXT,
        listing_provided_agent_number TEXT,
        listing_provided_office_name TEXT,
        listing_provided_office_email TEXT,
        listing_provided_office_number TEXT,
        parcel_number TEXT
    )
    """)
    conn.commit()


async def scrape_page(page):
    print(f"[scrape_page] Opening: {page.url}")
    res = {
        "link": page.url,
        "street": "",
        "zip": "",
        "price": 0,
        "beds": 0,
        "baths": 0,
        "sf1": 0,
        "sf2": 0,
        "year": 0,
        "property_and_building_type": "",
        "listing_provided_agent_name": "",
        "listing_provided_agent_email": "",
        "listing_provided_agent_number": "",
        "listing_provided_office_name": "",
        "listing_provided_office_email": "",
        "listing_provided_office_number": "",
        "parcel_number": "",
    }

    await page.wait_for_selector("article.summary", timeout=60_000)

    # Street & ZIP
    street = await page.locator("article.summary p.summary__address").inner_text()
    res["street"] = street.strip()
    zip_code = street.split(" ")[-1].strip()
    if zip_code.isdigit():
        res["zip"] = zip_code

    # Price
    price_raw = await page.locator(
        "article.summary p.summary__price.total-price"
    ).inner_text()
    price_clean = price_raw.replace("$", "").replace(",", "")
    if price_clean.isdigit():
        res["price"] = int(price_clean)

    # Beds / Baths / SF1 / SF2
    summaries = page.locator(
        "article.summary ul.summary__properties li.summary__property"
    )
    count_summary = await summaries.count()
    for i in range(count_summary):
        spans = summaries.nth(i).locator("span")
        key = (await spans.nth(1).inner_text()).strip()
        val = (await spans.nth(0).inner_text()).replace(",", "").strip()
        if val.isdigit():
            num = int(val)
            if key == "Beds":
                res["beds"] = num
            elif key == "Baths":
                res["baths"] = num
            elif key == "sqft":
                res["sf1"] = num
            elif key == "sqft lot":
                res["sf2"] = num

    # Year built / Home Type
    highlights = page.locator(
        "div.highlights__properties div.highlights__property"
    )
    count_high = await highlights.count()
    for i in range(count_high):
        label = (await highlights.nth(i)
                          .locator("div.highlights__property-label")
                          .inner_text()).strip()
        value = (await highlights.nth(i)
                          .locator("div.highlights__property-value")
                          .inner_text()).strip()
        if label == "Year built" and value.isdigit():
            res["year"] = int(value)
        elif label == "Home Type":
            res["property_and_building_type"] = value

    # Agent & Office info
    info = page.locator("article.listing-information")
    for section, prefix in [("agent", "listing_provided_agent_"),
                            ("office", "listing_provided_office_")]:
        lis = info.locator(f"div.listing-information__{section} ul li")
        count_li = await lis.count()
        for i in range(count_li):
            text = await lis.nth(i).inner_text()
            if "Name:" in text:
                res[prefix + "name"] = text.split(":", 1)[1].strip()
            elif "Email:" in text:
                res[prefix + "email"] = text.split(":", 1)[1].strip()
            elif "Phone:" in text:
                res[prefix + "number"] = text.split(":", 1)[1].strip()

    # Parcel number under “Exterior”
    others = page.locator("article.other-properties section.other-property")
    count_others = await others.count()
    for i in range(count_others):
        title = (await others.nth(i)
                          .locator("h6.other-property__title")
                          .inner_text()).strip()
        if title == "Exterior":
            items = others.nth(i).locator("div.other-property__item ul li")
            count_items = await items.count()
            for j in range(count_items):
                line = await items.nth(j).inner_text()
                if "Parcel Number:" in line:
                    num = line.split(":", 1)[1].strip()
                    if num.isdigit():
                        res["parcel_number"] = num

    insert_sql(res)
    print(res)
    return res


async def fetch_detail(context, url, sem, idx):
    async with sem:
        print(f"[fetch_detail] ({idx}) Visiting {url}")
        page = await context.new_page()
        try:
            await page.goto(url)
            return await scrape_page(page)
        finally:
            await page.close()


async def main():
    init_db()
    sem = asyncio.Semaphore(MAX_CONCURRENCY)
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context()
        page = await context.new_page()
        await page.goto(URL)
        all_results = []
        page.wait_for_load_state("networkidle", timeout=60_000)
        time.sleep(15)
        remove_boundary_button = page.locator("button.remove-boundary-btn")
        remove_boundary_button.click()

        page_idx = 1
        while True:
            print(f"[main] On results page #{page_idx}")
            await page.wait_for_selector("div.search-result__list a", timeout=60_000)
            hrefs = await page.eval_on_selector_all(
                "div.search-result__list a",
                "els => els.map(e => e.getAttribute('href'))"
            )

            tasks = []
            new_context = await browser.new_context()
            for i, href in enumerate(hrefs):
                if not href:
                    continue
                if "https" in href:
                    continue
                full = f"https://cozying.ai{href}"
                tasks.append(
                    asyncio.create_task(fetch_detail(new_context, full, sem, i))
                )

            # gather and print errors
            for result in await asyncio.gather(*tasks, return_exceptions=True):
                if isinstance(result, Exception):
                    print("[main] Error scraping detail:", result)
                else:
                    all_results.append(result)

            # Next page?
            next_btn = page.locator("nav.pagination li.pagination__nav").nth(-1)
            classes = (await next_btn.get_attribute("class")) or ""
            if "link-disabled" in classes:
                print("[main] No more pages—exiting loop.")
                break

            print("[main] Clicking Next →")
            await next_btn.click()
            await page.wait_for_timeout(6000)
            page_idx += 1

        await browser.close()

    df = pd.read_sql_query("SELECT * FROM listings", conn)
    conn.close()
    df.to_excel("properties-sell.xlsx", index=False)

if __name__ == "__main__":
    asyncio.run(main())
