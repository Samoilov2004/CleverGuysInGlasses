import aiohttp
import asyncio
from bs4 import BeautifulSoup
import random
import pandas as pd
import re
import json

MAX_CONCURRENT_REQUESTS = 30
MAX_RETRIES = 3
RETRY_DELAY = 10

async def parse_google_patent(semaphore, patent_id):
    url = f"https://patents.google.com/patent/{patent_id}/en"
    headers = {"User-Agent": "Mozilla/5.0"}
    async with semaphore:
        delay = random.uniform(1, 3)
        await asyncio.sleep(delay)
        text = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                async with aiohttp.ClientSession(headers=headers) as session:
                    async with session.get(url) as response:
                        if response.status != 200:
                            raise Exception(f"Error: {response.status}")
                        text = await response.text()
                break
            except Exception as e:
                if attempt == MAX_RETRIES:
                    print(f"Patent {patent_id}: failed after {MAX_RETRIES} attempts")
                    return {"id": patent_id, "text": ""}
                await asyncio.sleep(RETRY_DELAY)

    soup = BeautifulSoup(text, "html.parser")

    title_tag = soup.find("span", {"itemprop": "title"})
    title = title_tag.text.strip() if title_tag else ""

    abstract_tag = soup.find("section", {"itemprop": "abstract"})
    abstract = abstract_tag.get_text(strip=True) if abstract_tag else ""

    claims_tag = soup.find("section", {"itemprop": "claims"})
    claims = claims_tag.get_text(separator="\n").strip() if claims_tag else ""

    description_tag = soup.find("section", {"itemprop": "description"})
    description = description_tag.get_text(separator="\n").strip() if description_tag else ""

    joined_text = " ".join([title, abstract, claims, description]).strip()
    return {"id": patent_id, "text": joined_text}

async def process_batch(batch, pattern, batch_num):
    result = []
    for patent in batch:
        if not patent or not patent["text"]:
            continue
        if pattern.search(patent["text"]):
            result.append({"id": patent["id"], "text": patent["text"]})
    if result:
        output_filename = f"/Users/samoilov2004/Desktop/google_parser/output/output_batch_{batch_num:05d}.json"
        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"Batch {batch_num}: {len(result)} matches -> saved {output_filename}")
    else:
        print(f"Batch {batch_num}: no matches")

async def parse_google_patent_main(patent_ids, pattern, batch_size=100):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    batch_num = 1
    for i in range(0, len(patent_ids), batch_size):
        batch_patent_ids = patent_ids[i:i+batch_size]
        tasks = [asyncio.create_task(parse_google_patent(semaphore, pid)) for pid in batch_patent_ids]
        batch_results = await asyncio.gather(*tasks)
        await process_batch(batch_results, pattern, batch_num)
        batch_num += 1

if __name__ == "__main__":
    patent_ids = pd.read_csv('filtered_patents_mini.csv')['patent_number'].tolist()
    clean_patent_ids = [pid.replace("-", "") for pid in patent_ids[130001:160000]]

    sub_5 = "₅"
    sub_0 = "₀"


    digits = r'(?:50|{}{})?'.format(sub_5, sub_0)  # Опциональные цифры или субскрипты после IC/EC

    pattern_str = (
        r'(?:'
            r'\bIC' + digits + r'(?:\s*[\(\)]*\s*nM\s*[\)\(]*)?'
            r'|\bEC' + digits + r'(?:\s*[\(\)]*\s*nM\s*[\)\(]*)?'
            r'|\bKi' + r'(?:\d+)?(?:\s*[\(\)]*\s*nM\s*[\)\(]*)?'   # Ki, Ki50, Ki222, Ki (nM)
            r'|\bKd' + r'(?:\d+)?(?:\s*[\(\)]*\s*nM\s*[\)\(]*)?'   # Kd, Kd50 и т.д.
        r')\b'
    )

    regex = re.compile(pattern_str, flags=re.IGNORECASE)

    asyncio.run(parse_google_patent_main(clean_patent_ids, regex, batch_size=100))
