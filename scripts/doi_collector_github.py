"""
doi_collector_github.py — GitHub Actions'ta bagimsiz calisir.
Bagimlilik: sadece httpx (pip install httpx)
Cikti: data/_master.jsonl + data/progress.json
"""
import asyncio, json, time, logging, os, sys
from pathlib import Path
import httpx

# GitHub Actions ortaminda calis
DATA_DIR    = Path("data")
MASTER_FILE = DATA_DIR / "_master.jsonl"
PROGRESS    = DATA_DIR / "progress.json"
LOG_FILE    = DATA_DIR / "collector.log"
DATA_DIR.mkdir(exist_ok=True)

OA_EMAIL   = "emrecancerli55@gmail.com"
OA_HEADERS = {"User-Agent": f"ZyntraResearch/3.0 (mailto:{OA_EMAIL})"}
PER_PAGE   = 200
MAX_PAGES  = 50

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("doi")

# Taxonomy'den sorgu uret
def load_queries():
    tax_file = Path("Rocket/research_taxonomy.json")
    if not tax_file.exists():
        log.error("Taxonomy bulunamadi!")
        return []
    tax = json.loads(tax_file.read_text(encoding="utf-8-sig"))
    queries = []
    for disc_key, disc in tax.get("disciplines", {}).items():
        if disc.get("deferred_fulltext"):
            continue
        for sub_key, sub in disc.get("subtopics", {}).items():
            if "papers" in sub.get("sources", []):
                for q in sub.get("queries", []):
                    queries.append((disc_key, sub_key, q))
    return queries

# DOI Store
_seen = set()
_progress = {}
_master_f = None

def _load():
    global _seen, _progress
    if MASTER_FILE.exists():
        for line in MASTER_FILE.open(encoding="utf-8"):
            try: _seen.add(json.loads(line)["doi"])
            except: pass
        log.info(f"Resume: {len(_seen):,} mevcut DOI")
    if PROGRESS.exists():
        try: _progress = json.loads(PROGRESS.read_text())
        except: pass
    done = sum(1 for v in _progress.values() if v)
    log.info(f"Progress: {done}/{len(_progress)} tamamlanmis")

def _append(doi_obj, disc, sub):
    global _master_f
    doi = doi_obj.get("doi", "").strip()
    if not doi or doi in _seen: return False
    _seen.add(doi)
    rec = json.dumps({**doi_obj, "disc": disc, "sub": sub, "ts": int(time.time())}, ensure_ascii=False)
    if _master_f is None:
        _master_f = open(MASTER_FILE, "a", encoding="utf-8")
    _master_f.write(rec + "\n")
    _master_f.flush()
    return True

def _save_progress():
    PROGRESS.write_text(json.dumps(_progress), encoding="utf-8")

def _mark_done(key):
    _progress[key] = True
    _save_progress()

# OpenAlex
async def fetch_oa(client, query, disc, sub):
    key = f"OA::{disc}::{sub}::{query[:40]}"
    if _progress.get(key): return 0
    cursor, new = "*", 0
    for page in range(MAX_PAGES):
        url = (f"https://api.openalex.org/works"
               f"?search={query.replace(' ','+')}&per-page={PER_PAGE}"
               f"&cursor={cursor}&select=doi,title,abstract_inverted_index,"
               f"authorships,publication_year,cited_by_count,concepts,"
               f"primary_location,open_access,referenced_works_count,language")
        for attempt in range(3):
            try:
                r = await client.get(url, headers=OA_HEADERS, timeout=30)
                if r.status_code == 429:
                    log.warning(f"OA-429 [{query[:25]}] — 60s bekle")
                    await asyncio.sleep(60); continue
                if r.status_code != 200: _mark_done(key); return new
                data = r.json()
                results = data.get("results", [])
                if not results: _mark_done(key); return new
                for w in results:
                    raw = (w.get("doi") or "").strip().replace("https://doi.org/", "")
                    if not raw: continue
                    inv = w.get("abstract_inverted_index") or {}
                    pos = {}
                    for word, pl in inv.items():
                        for p in pl: pos[p] = word
                    abstract = " ".join(pos[k] for k in sorted(pos))
                    loc = w.get("primary_location") or {}
                    src = loc.get("source") or {}
                    oa  = w.get("open_access") or {}
                    auths = [a.get("author", {}).get("display_name", "") for a in (w.get("authorships") or [])][:8]
                    concepts = [c.get("display_name", "") for c in (w.get("concepts") or [])][:5]
                    obj = {
                        "doi": raw, "title": (w.get("title") or "")[:500],
                        "abstract": abstract[:2000], "authors": auths,
                        "year": w.get("publication_year"),
                        "journal": (src.get("display_name") or "")[:200],
                        "cited_by": w.get("cited_by_count", 0), "concepts": concepts,
                        "is_oa": oa.get("is_oa", False), "oa_url": oa.get("oa_url", ""),
                        "ref_count": w.get("referenced_works_count", 0),
                        "language": w.get("language", ""), "source": "openalex",
                    }
                    if _append(obj, disc, sub): new += 1
                meta = data.get("meta", {})
                cursor = meta.get("next_cursor", "")
                if not cursor: _mark_done(key); return new
                break
            except Exception as e:
                log.warning(f"OA-ERR [{query[:25]}] p{page}: {e}")
                await asyncio.sleep(15)
        await asyncio.sleep(0.5)
    _mark_done(key)
    return new

# Crossref
async def fetch_cr(client, query, disc, sub):
    key = f"CR::{disc}::{sub}::{query[:40]}"
    if _progress.get(key): return 0
    new, offset = 0, 0
    while offset <= 4000:
        url = (f"https://api.crossref.org/works"
               f"?query={query.replace(' ','+')}&rows=200&offset={offset}&mailto={OA_EMAIL}")
        for attempt in range(3):
            try:
                r = await client.get(url, timeout=30)
                if r.status_code == 429:
                    await asyncio.sleep(30); continue
                if r.status_code != 200: _mark_done(key); return new
                items = r.json().get("message", {}).get("items", [])
                if not items: _mark_done(key); return new
                for item in items:
                    raw = (item.get("DOI") or "").strip()
                    if not raw: continue
                    titles = item.get("title") or [""]
                    auths = [f"{a.get('given','')} {a.get('family','')}".strip()
                             for a in (item.get("author") or [])][:8]
                    issued = (item.get("issued", {}).get("date-parts") or [[None]])[0]
                    year = issued[0] if issued else None
                    journal = (item.get("container-title") or [""])[0][:200]
                    obj = {
                        "doi": raw, "title": (titles[0] if titles else "")[:500],
                        "abstract": (item.get("abstract") or "")[:2000], "authors": auths,
                        "year": year, "journal": journal,
                        "cited_by": item.get("is-referenced-by-count", 0),
                        "concepts": [], "is_oa": False, "oa_url": "",
                        "ref_count": item.get("references-count", 0),
                        "language": "", "source": "crossref",
                    }
                    if _append(obj, disc, sub): new += 1
                break
            except Exception as e:
                log.warning(f"CR-ERR [{query[:25]}] off={offset}: {e}")
                await asyncio.sleep(15)
        offset += 200
        await asyncio.sleep(0.3)
    _mark_done(key)
    return new

# EuropePMC
async def fetch_epmc(client, query, disc, sub):
    key = f"EPMC::{disc}::{sub}::{query[:40]}"
    if _progress.get(key): return 0
    new, cursor = 0, "*"
    for page in range(15):
        url = (f"https://www.ebi.ac.uk/europepmc/webservices/rest/search"
               f"?query={query.replace(' ','+')}&format=json&pageSize=200"
               f"&cursorMark={cursor}&resultType=core&sort=CITED+desc")
        for attempt in range(3):
            try:
                r = await client.get(url, timeout=30)
                if r.status_code == 429:
                    await asyncio.sleep(30); continue
                if r.status_code != 200: _mark_done(key); return new
                data = r.json()
                results = data.get("resultList", {}).get("result", [])
                if not results: _mark_done(key); return new
                for item in results:
                    raw = (item.get("doi") or "").strip()
                    if not raw: continue
                    auths = [a.get("fullName", "") for a in
                             (item.get("authorList", {}).get("author") or [])][:8]
                    obj = {
                        "doi": raw, "title": (item.get("title") or "")[:500],
                        "abstract": (item.get("abstractText") or "")[:2000],
                        "authors": auths, "year": item.get("pubYear"),
                        "journal": (item.get("journalTitle") or "")[:200],
                        "cited_by": item.get("citedByCount", 0),
                        "concepts": [], "is_oa": item.get("isOpenAccess", "N") == "Y",
                        "oa_url": "", "ref_count": 0, "language": "", "source": "europepmc",
                    }
                    if _append(obj, disc, sub): new += 1
                next_c = data.get("nextCursorMark", "")
                if not next_c or next_c == cursor: _mark_done(key); return new
                cursor = next_c
                break
            except Exception as e:
                log.warning(f"EPMC-ERR [{query[:25]}] p{page}: {e}")
                await asyncio.sleep(15)
        await asyncio.sleep(0.5)
    _mark_done(key)
    return new

# Semantic Scholar
async def fetch_s2(client, query, disc, sub):
    key = f"S2::{disc}::{sub}::{query[:40]}"
    if _progress.get(key): return 0
    new, offset = 0, 0
    fields = "externalIds,title,abstract,year,authors,citationCount,openAccessPdf,publicationVenue"
    while offset <= 1900:
        url = (f"https://api.semanticscholar.org/graph/v1/paper/search"
               f"?query={query.replace(' ','+')}&offset={offset}&limit=100&fields={fields}")
        for attempt in range(3):
            try:
                r = await client.get(url, timeout=30)
                if r.status_code == 429:
                    await asyncio.sleep(60); continue
                if r.status_code != 200: _mark_done(key); return new
                data = r.json()
                papers = data.get("data", [])
                if not papers: _mark_done(key); return new
                for paper in papers:
                    ext = paper.get("externalIds") or {}
                    raw = (ext.get("DOI") or "").strip()
                    if not raw: continue
                    auths = [a.get("name", "") for a in (paper.get("authors") or [])][:8]
                    venue = paper.get("publicationVenue") or {}
                    oa_pdf = paper.get("openAccessPdf") or {}
                    obj = {
                        "doi": raw, "title": (paper.get("title") or "")[:500],
                        "abstract": (paper.get("abstract") or "")[:2000],
                        "authors": auths, "year": paper.get("year"),
                        "journal": (venue.get("name") or "")[:200],
                        "cited_by": paper.get("citationCount", 0),
                        "concepts": [], "is_oa": bool(oa_pdf.get("url")),
                        "oa_url": oa_pdf.get("url", ""),
                        "ref_count": 0, "language": "", "source": "s2",
                    }
                    if _append(obj, disc, sub): new += 1
                break
            except Exception as e:
                log.warning(f"S2-ERR [{query[:25]}] off={offset}: {e}")
                await asyncio.sleep(15)
        offset += 100
        await asyncio.sleep(1.2)
    _mark_done(key)
    return new

# Ana dongu
async def run():
    _load()
    queries = load_queries()
    log.info(f"{'='*60}")
    log.info(f"GitHub Actions DOI Collector — {len(queries)} sorgu x 4 kaynak")
    log.info(f"Baslangic DOI: {len(_seen):,}")
    log.info(f"{'='*60}")

    OA_SEM = asyncio.Semaphore(20)
    CR_SEM = asyncio.Semaphore(25)
    EP_SEM = asyncio.Semaphore(15)
    S2_SEM = asyncio.Semaphore(5)

    async def _oa(c, q, d, s):
        async with OA_SEM: return await fetch_oa(c, q, d, s)
    async def _cr(c, q, d, s):
        async with CR_SEM: return await fetch_cr(c, q, d, s)
    async def _ep(c, q, d, s):
        async with EP_SEM: return await fetch_epmc(c, q, d, s)
    async def _s2(c, q, d, s):
        async with S2_SEM: return await fetch_s2(c, q, d, s)

    total_new = 0
    async with httpx.AsyncClient(follow_redirects=True) as client:
        tasks = []
        for disc, sub, query in queries:
            tasks.append(_oa(client, query, disc, sub))
            tasks.append(_cr(client, query, disc, sub))
            tasks.append(_ep(client, query, disc, sub))
            tasks.append(_s2(client, query, disc, sub))

        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, int): total_new += r

    if _master_f:
        _master_f.close()

    # Ozet dosya yaz
    summary = {
        "run_time": time.strftime("%Y-%m-%d %H:%M UTC"),
        "total_doi": len(_seen),
        "new_this_run": total_new,
        "queries_run": len(queries),
    }
    (DATA_DIR / "summary.json").write_text(json.dumps(summary, indent=2))

    log.info(f"{'='*60}")
    log.info(f"TAMAMLANDI — Bu calisma: +{total_new:,} yeni DOI")
    log.info(f"Toplam: {len(_seen):,} DOI")
    log.info(f"{'='*60}")
    print(f"\nToplam {len(_seen):,} DOI | Bu run: +{total_new:,}")

if __name__ == "__main__":
    asyncio.run(run())
