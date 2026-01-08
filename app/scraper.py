import aiohttp
from bs4 import BeautifulSoup

async def scrape_url(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(
            url,
            headers={"User-Agent": "Mozilla/5.0"}
        ) as response:
            html = await response.text()

    soup = BeautifulSoup(html, "html.parser")

    return {
        "url": url,
        "title": soup.title.text if soup.title else ""
    }
