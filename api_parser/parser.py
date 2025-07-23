import asyncio
import re
import pandas as pd
import json
from tqdm import tqdm
import aiohttp
import async_timeout
import time
from typing import List

class DocumentFetcher:
    def __init__(self, concurrency_limit: int = 5, max_retries: int = 5, base_delay: float = 10.0):
        self.semaphore = asyncio.Semaphore(concurrency_limit)
        self.max_retries = max_retries
        self.base_delay = base_delay

    async def fetch_document(self, session: aiohttp.ClientSession, doc_id: str) -> dict:
        url = f"https://surechembl.org/api/document/{doc_id}/contents"
        headers = {'Content-Type': 'application/json'}

        for attempt in range(1, self.max_retries + 1):
            async with self.semaphore:
                try:
                    async with async_timeout.timeout(60):  # Внешний timeout на всю операцию
                        async with session.get(url, headers=headers) as response:
                            if response.status == 200:
                                try:
                                    return await response.json()
                                except Exception as e:
                                    print(f"JSON parse error for {doc_id}: {e}")
                                    return None
                            else:
                                print(f"Error fetching {doc_id}: HTTP {response.status}")
                    # eсли не 200, то retry
                except asyncio.TimeoutError:
                    print(f"Timeout when fetching {doc_id} (attempt {attempt})")
                except Exception as e:
                    print(f"Exception when fetching {doc_id} (attempt {attempt}): {str(e)}")

                if attempt < self.max_retries:
                    sleep_time = self.base_delay * (2 ** (attempt - 1))
                    print(f"Retrying {doc_id} after {sleep_time:.1f} seconds...")
                    await asyncio.sleep(sleep_time)
        print(f"Failed to fetch {doc_id} after {self.max_retries} attempts")
        return None

    async def fetch_multiple_documents(self, doc_ids: List[str]) -> List[dict]:
        async with aiohttp.ClientSession() as session:
            tasks = [self.fetch_document(session, doc_id) for doc_id in doc_ids]
            return await asyncio.gather(*tasks)

async def fetch_document_main(document_ids, concurrency_limit):
    fetcher = DocumentFetcher(concurrency_limit)
    results = await fetcher.fetch_multiple_documents(document_ids)
    res_api = {}

    for doc_id, result in zip(document_ids, results):
        if result is not None:
            try:
                data = result['data']['contents']['patentDocument']
                title = next((item for item in data['bibliographicData']['technicalData']['inventionTitles'] if item['lang'] == 'EN'), None)
                abstract = next((item for item in data.get('abstracts', []) if item['lang'] == 'EN'), None)
                claims = next((item for item in data.get('claimResponses', []) if item['lang'] == 'EN'), None)
                descriptions = next((item for item in data.get('descriptions', []) if item['lang'] == 'EN'), None)

                def safe_section(val):
                    if val and 'section' in val and 'content' in val['section'] and 'annotations' in val['section']:
                        return {'text': val['section']['content'],
                                'chem': val['section']['annotations']}
                    return None

                res_api[doc_id] = {
                    'title': {
                        'text': title['title'] if title else None,
                        'chem': title.get('annotation', {}).get('chemicalAnnotations') if title else None,
                    },
                    'abstract': safe_section(abstract),
                    'claims': safe_section(claims),
                    'descriptions': safe_section(descriptions),
                }
            except Exception as e:
                print(f"Parsing error for {doc_id}: {e}")
                res_api[doc_id] = None
        else:
            print(f"\nFailed to fetch {doc_id}")

    return res_api

patent_ids = pd.read_csv('filtered_patents.csv')['patent_number'].tolist()

async def process_documents(document_ids, concurrency_limit,
                           constant_regex,
                           success_log='success.log', error_log='error.log'):
    results = await fetch_document_main(document_ids, concurrency_limit)
    all_texts = {}

    with open(success_log, 'a', encoding='utf-8') as success_f, open(error_log, 'a', encoding='utf-8') as error_f:
        for doc_id, doc_info in tqdm(results.items(), total=len(results), desc="Processing documents"):
            if doc_info is None:
                error_f.write(f"{doc_id}\tERROR\n")
                error_f.flush()
                continue

            section_texts = []
            chem_names = set()
            for section_key in ['title', 'abstract', 'claims', 'descriptions']:
                section = doc_info.get(section_key)
                if section:
                    if section.get('text'):
                        section_texts.append(section['text'])
                    if section.get('chem'):
                        for chem in section['chem']:
                            name = chem.get('name')
                            if name:
                                chem_names.add(name)
            full_text = "\n\n".join(section_texts)

            if constant_regex.search(full_text):
                pattern_status = 'PATTERN_FOUND'
                all_texts[doc_id] = {
                    'text': full_text,
                    'chem': sorted(chem_names)
                }
            else:
                pattern_status = 'NO_PATTERN_FOUND'
            success_f.write(f"{doc_id}\t{pattern_status}\n")
            success_f.flush()

    return all_texts

# Парсер v3
sub_5 = "₅"
sub_0 = "₀"
digits = r'(?:50|₅₀)'

pattern = (
    r'(?:'
    r'\bIC' + digits + r'\s*\(\s*nM\s*\)'
    r'|\bEC' + digits + r'\s*\(\s*nM\s*\)'
    r'|\bKi\s*\(\s*nM\s*\)'
    r'|\bKd\s*\(\s*nM\s*\)'
    r')'
)
regex = re.compile(pattern)

# Не удалять что снизу, запускаю с локального
async def main():
    res = dict()
    batch_size = 100
    num_docs = len(patent_ids[10000:12000])
    for i in range(0, num_docs, batch_size):
        batch_ids = patent_ids[43 + i:43 + i + batch_size]
        batch_res = await process_documents(batch_ids, 25, regex)
        res.update(batch_res)
        with open('found_chunks_checkpoint.json', 'w', encoding='utf-8') as fout:
            json.dump(res, fout, ensure_ascii=False, indent=2)
    with open('found_chunks.json', 'w', encoding='utf-8') as fout:
        json.dump(res, fout, ensure_ascii=False, indent=2)

if __name__ == '__main__':
    asyncio.run(main())
