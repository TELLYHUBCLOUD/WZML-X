from base64 import b64encode
from random import choice, random
from asyncio import sleep as asleep
from urllib.parse import quote

from cloudscraper import create_scraper
from urllib3 import disable_warnings

from ... import LOGGER, shortener_dict


# Cloudflare Worker URL
WORKER_URL = "https://tellylinks.tellycloudapi.workers.dev/shorten"

async def short_url(longurl, attempt=0):
    """
    Shortens URL using priority:
    1. Cloudflare Worker (Primary)
    2. Old shorteners (Fallback)
    3. Return original URL
    """
    if not longurl:
        return longurl

    # Max retry check
    if attempt >= 4:
        LOGGER.warning(f"Max attempts reached for: {longurl}")
        return longurl

    disable_warnings()
    cget = create_scraper().request

    # -----------------------------------------------------------
    # 🔹 METHOD 1: Cloudflare Worker (Primary Shortener)
    # -----------------------------------------------------------
    try:
        async with ClientSession() as session:
            async with session.get(f"{WORKER_URL}?url={quote(longurl)}", timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("success"):
                        shorted = data.get("shortUrl")
                        if shorted:
                            LOGGER.info(f"Worker Shortened → {shorted}")
                            return shorted
    except Exception as e:
        LOGGER.error(f"Worker shortening failed: {e}")

    # If no shortener dict, return longurl
    if not shortener_dict:
        return longurl

    # -----------------------------------------------------------
    # 🔹 METHOD 2: Old Shortener System (Fallback)
    # -----------------------------------------------------------
    _shortener, _shortener_api = choice(list(shortener_dict.items()))

    try:
        if "shorte.st" in _shortener:
            headers = {"public-api-token": _shortener_api}
            data = {"urlToShorten": quote(longurl)}
            return cget("PUT", "https://api.shorte.st/v1/data/url", headers=headers, data=data).json()["shortenedUrl"]

        elif "linkvertise" in _shortener:
            enc = quote(b64encode(longurl.encode()).decode())
            urls = [
                f"https://link-to.net/{_shortener_api}/{random()*1000}/dynamic?r={enc}",
                f"https://up-to-down.net/{_shortener_api}/{random()*1000}/dynamic?r={enc}",
                f"https://direct-link.net/{_shortener_api}/{random()*1000}/dynamic?r={enc}",
                f"https://file-link.net/{_shortener_api}/{random()*1000}/dynamic?r={enc}",
            ]
            return choice(urls)

        elif "bitly.com" in _shortener:
            headers = {"Authorization": f"Bearer {_shortener_api}"}
            return cget("POST", "https://api-ssl.bit.ly/v4/shorten", json={"long_url": longurl}, headers=headers).json()["link"]

        elif "ouo.io" in _shortener:
            return cget("GET", f"http://ouo.io/api/{_shortener_api}?s={longurl}", verify=False).text

        elif "cutt.ly" in _shortener:
            return cget("GET", f"http://cutt.ly/api/api.php?key={_shortener_api}&short={longurl}").json()["url"]["shortLink"]

        else:
            res = cget("GET", f"https://{_shortener}/api?api={_shortener_api}&url={quote(longurl)}").json()
            shorted = res.get("shortenedUrl")

            # fallback → use shrtco internally
            if not shorted:
                sc = cget("GET", f"https://api.shrtco.de/v2/shorten?url={quote(longurl)}").json()
                sc_link = sc["result"]["full_short_link"]
                res2 = cget("GET", f"https://{_shortener}/api?api={_shortener_api}&url={sc_link}").json()
                shorted = res2.get("shortenedUrl") or longurl

            return shorted

    except Exception as e:
        LOGGER.error(e)
        await asleep(0.8)
        return await short_url(longurl, attempt + 1)

