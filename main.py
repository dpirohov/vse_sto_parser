import asyncio
import json
from itertools import chain

import aiohttp
import pandas as pd
from bs4 import BeautifulSoup

PAGES = range(1, 247)

LINK = "https://vse-sto.com.ua/kiev/sto?page={}"


async def get_page_data(page: int, semaphore: asyncio.Semaphore):
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            response = await session.get(LINK.format(page))
            text = await response.text()
    bs4 = BeautifulSoup(text, "html.parser")
    urls = []
    stations = bs4.find_all("li", {"class": "white-block-small service-item"})
    for station in stations:
        link_element = station.find("a", {"class": "service-item-link"})
        url = link_element.get("href")
        urls.append(url)
    return urls


async def get_data_by_link(link: str):
    print(link)
    async with aiohttp.ClientSession() as session:
        response = await session.get(link)
        text = await response.text()
    bs4 = BeautifulSoup(text, "html.parser")

    title = bs4.find("h1", {"class": "company-title"})
    title = title.text if title is not None else title

    phone_numbers = [data.text for data in bs4.find_all("span", {"class": "number"})]

    address = bs4.find("div", {"class": "address-info"})
    address = address.text if address is not None else address

    sto_url = bs4.find("a", {"class": "website-url"})
    sto_url = sto_url.text if sto_url is not None else sto_url

    print(link, "OK")

    return {
        "title": title,
        "phone_numbers": phone_numbers,
        "address": address,
        "sto_urs": sto_url,
        "page_url": link,
    }


async def main():
    semaphore = asyncio.Semaphore(20)
    urls = await asyncio.gather(*[get_page_data(page, semaphore) for page in PAGES])

    all_pages = list(chain(*urls))
    json_data = json.dumps(all_pages)

    with open("urls.txt", "w") as f:
        f.write(json_data)

    with open("urls.txt", "r") as f:
        urls = json.loads(f.read())

    data = await asyncio.gather(*[get_data_by_link(url) for url in urls])

    df = pd.DataFrame(data=data)
    df.to_excel('output.xlsx', index=False)

if __name__ == "__main__":
    asyncio.run(main())
