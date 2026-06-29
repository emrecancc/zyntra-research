"""
doi_collector_slice.py — Paralel job için slice-tabanlı DOI toplayici.
Her GitHub Actions job farklı --job-id ile çalışır.
Kendi progress ve output dosyalarını yazar (data/progress_NN.json, data/summary_NN.json)
Ana _master.jsonl'a da ekler (deduplication ile).
"""
import asyncio, json, time, logging, sys, argparse
from pathlib import Path
import httpx

parser = argparse.ArgumentParser()
parser.add_argument("--job-id",     type=int, default=0)
parser.add_argument("--total-jobs", type=int, default=20)
args = parser.parse_args()

JOB_ID     = args.job_id
TOTAL_JOBS = args.total_jobs

# Her slice kendi alt klasorune yazar → git conflict olmaz
DATA_DIR    = Path(f"data/slice_{JOB_ID:02d}")
MASTER_FILE = DATA_DIR / "dois.jsonl"        # sadece bu slice'in DOI'leri
PROGRESS    = DATA_DIR / "progress.json"
SUMMARY     = DATA_DIR / "summary.json"
LOG_FILE    = DATA_DIR / "collector.log"
DATA_DIR.mkdir(parents=True, exist_ok=True)

OA_EMAIL   = "emrecancerli55@gmail.com"
OA_HEADERS = {"User-Agent": f"ZyntraResearch/3.0 (mailto:{OA_EMAIL})"}
PER_PAGE   = 200
MAX_PAGES  = 25
HARD_STOP  = time.time() + 23 * 60

logging.basicConfig(
    level=logging.INFO,
    format=f"%(asctime)s J{JOB_ID:02d} %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(f"j{JOB_ID:02d}")

# ── Taxonomy slice ────────────────────────────────────────────
def load_queries():
    # Oncelikle slice dosyasini dene
    slice_file = Path(f"scripts/query_slices/slice_{JOB_ID:02d}.json")
    if slice_file.exists():
        data = json.loads(slice_file.read_text(encoding="utf-8"))
        log.info(f"Slice dosyasi: {len(data)} query")
        return [(d, s, q) for d, s, q in data]

    # Yoksa taxonomy'den hesapla
    import math
    tax_paths = [
        Path("Rocket/research_taxonomy.json"),
        Path("research_taxonomy.json"),
    ]
    tax_file = next((p for p in tax_paths if p.exists()), None)
    if not tax_file:
        log.error("Taxonomy bulunamadi!")
        return []
    tax = json.loads(tax_file.read_text(encoding="utf-8-sig"))
    all_queries = []
    for dk, dv in tax.get("disciplines", {}).items():
        if dv.get("deferred_fulltext"): continue
        for sk, sv in dv.get("subtopics", {}).items():
            if "papers" in sv.get("sources", []):
                for q in sv.get("queries", []):
                    all_queries.append((dk, sk, q))
    total = len(all_queries)
    slice_size = math.ceil(total / TOTAL_JOBS)
    start = JOB_ID * slice_size
    end   = min(start + slice_size, total)
    chunk = all_queries[start:end]
    log.info(f"Taxonomy'den slice: {start}-{end} ({len(chunk)} query)")
    return chunk

# ── Store ─────────────────────────────────────────────────────
_seen      = set()
_progress  = {}
_master_f  = None
_new_count = 0

def _load():
    global _seen, _progress
    # Bu slice'in kendi doi dosyasindan mevcut DOI'leri yukle
    if MASTER_FILE.exists():
        for line in MASTER_FILE.open(encoding="utf-8", errors="ignore"):
            try: _seen.add(json.loads(line)["doi"])
            except: pass
        log.info(f"Bu slice mevcut DOI: {len(_seen):,}")
    # Progress yukle
    if PROGRESS.exists():
        try: _progress = json.loads(PROGRESS.read_text())
        except: pass
    done = sum(1 for v in _progress.values() if v)
    log.info(f"Progress: {done}/{len(_progress)} tamamlanmis")

def _append(doi_obj, disc, sub):
    global _master_f, _new_count
    doi = doi_obj.get("doi","").strip()
    if not doi or doi in _seen: return False
    _seen.add(doi)
    rec = json.dumps({**doi_obj, "disc": disc, "sub": sub,
                      "job": JOB_ID, "ts": int(time.time())},
                     ensure_ascii=False)
    if _master_f is None:
        _master_f = open(MASTER_FILE, "a", encoding="utf-8")
    _master_f.write(rec + "\n")
    _master_f.flush()
    _new_count += 1
    if _new_count % 100 == 0:
        _save_summary()
    return True

def _mark_done(key):
    _progress[key] = True
    if sum(1 for v in _progress.values() if v) % 10 == 0:
        PROGRESS.write_text(json.dumps(_progress))

def _save_progress():
    PROGRESS.write_text(json.dumps(_progress))

def _save_summary():
    SUMMARY.write_text(json.dumps({
        "job_id": JOB_ID,
        "run_time": time.strftime("%Y-%m-%d %H:%M UTC"),
        "total_doi": len(_seen),
        "new_this_run": _new_count,
        "queries_done": sum(1 for v in _progress.values() if v),
        "queries_total": len(_progress),
    }, indent=2))

def _time_ok():
    return time.time() < HARD_STOP

# ── OpenAlex ─────────────────────────────────────────────────
async def fetch_oa(client, query, disc, sub):
    if not _time_ok(): return 0
    key = f"OA::{disc}::{sub}::{query[:40]}"
    if _progress.get(key): return 0
    cursor, new = "*", 0
    for _ in range(MAX_PAGES):
        if not _time_ok(): break
        url = (f"https://api.openalex.org/works"
               f"?search={query.replace(' ','+')}&per-page={PER_PAGE}"
               f"&cursor={cursor}&select=doi,title,abstract_inverted_index,"
               f"authorships,publication_year,cited_by_count,concepts,"
               f"primary_location,open_access,referenced_works_count,language")
        for _ in range(3):
            try:
                r = await client.get(url, headers=OA_HEADERS, timeout=30)
                if r.status_code == 429: await asyncio.sleep(30); continue
                if r.status_code != 200: _mark_done(key); return new
                data = r.json()
                results = data.get("results", [])
                if not results: _mark_done(key); return new
                for w in results:
                    raw = (w.get("doi") or "").replace("https://doi.org/","").strip()
                    if not raw: continue
                    inv = w.get("abstract_inverted_index") or {}
                    pos = {}
                    for word, pl in inv.items():
                        for p in pl: pos[p] = word
                    abstract = " ".join(pos[k] for k in sorted(pos))
                    loc = w.get("primary_location") or {}
                    src = loc.get("source") or {}
                    oa  = w.get("open_access") or {}
                    _append({
                        "doi": raw,
                        "title": (w.get("title") or "")[:500],
                        "abstract": abstract[:2000],
                        "authors": [a.get("author",{}).get("display_name","")
                                    for a in (w.get("authorships") or [])][:8],
                        "year": w.get("publication_year"),
                        "journal": (src.get("display_name") or "")[:200],
                        "cited_by": w.get("cited_by_count", 0),
                        "concepts": [c.get("display_name","")
                                     for c in (w.get("concepts") or [])][:5],
                        "is_oa": oa.get("is_oa", False),
                        "oa_url": oa.get("oa_url",""),
                        "ref_count": w.get("referenced_works_count", 0),
                        "language": w.get("language",""),
                        "source": "openalex",
                    }, disc, sub)
                    new += 1
                cursor = data.get("meta",{}).get("next_cursor","")
                if not cursor: _mark_done(key); return new
                break
            except: await asyncio.sleep(10)
        await asyncio.sleep(0.3)
    _mark_done(key); return new

# ── Crossref ─────────────────────────────────────────────────
async def fetch_cr(client, query, disc, sub):
    if not _time_ok(): return 0
    key = f"CR::{disc}::{sub}::{query[:40]}"
    if _progress.get(key): return 0
    new, offset = 0, 0
    while offset <= 4000 and _time_ok():
        url = (f"https://api.crossref.org/works"
               f"?query={query.replace(' ','+')}&rows=200&offset={offset}&mailto={OA_EMAIL}")
        for _ in range(3):
            try:
                r = await client.get(url, timeout=30)
                if r.status_code == 429: await asyncio.sleep(20); continue
                if r.status_code != 200: _mark_done(key); return new
                items = r.json().get("message",{}).get("items",[])
                if not items: _mark_done(key); return new
                for item in items:
                    raw = (item.get("DOI") or "").strip()
                    if not raw: continue
                    titles = item.get("title") or [""]
                    auths  = [f"{a.get('given','')} {a.get('family','')}".strip()
                              for a in (item.get("author") or [])][:8]
                    issued = (item.get("issued",{}).get("date-parts") or [[None]])[0]
                    _append({
                        "doi": raw,
                        "title": (titles[0] if titles else "")[:500],
                        "abstract": (item.get("abstract") or "")[:2000],
                        "authors": auths,
                        "year": issued[0] if issued else None,
                        "journal": (item.get("container-title") or [""])[0][:200],
                        "cited_by": item.get("is-referenced-by-count", 0),
                        "concepts": [], "is_oa": False, "oa_url": "",
                        "ref_count": item.get("references-count", 0),
                        "language": "", "source": "crossref",
                    }, disc, sub)
                    new += 1
                break
            except: await asyncio.sleep(10)
        offset += 200
        await asyncio.sleep(0.2)
    _mark_done(key); return new

# ── EuropePMC ────────────────────────────────────────────────
async def fetch_epmc(client, query, disc, sub):
    if not _time_ok(): return 0
    key = f"EPMC::{disc}::{sub}::{query[:40]}"
    if _progress.get(key): return 0
    new, cursor = 0, "*"
    for _ in range(15):
        if not _time_ok(): break
        url = (f"https://www.ebi.ac.uk/europepmc/webservices/rest/search"
               f"?query={query.replace(' ','+')}&format=json&pageSize=200"
               f"&cursorMark={cursor}&resultType=core&sort=CITED+desc")
        for _ in range(3):
            try:
                r = await client.get(url, timeout=30)
                if r.status_code == 429: await asyncio.sleep(20); continue
                if r.status_code != 200: _mark_done(key); return new
                data = r.json()
                results = data.get("resultList",{}).get("result",[])
                if not results: _mark_done(key); return new
                for item in results:
                    raw = (item.get("doi") or "").strip()
                    if not raw: continue
                    auths = [a.get("fullName","") for a in
                             (item.get("authorList",{}).get("author") or [])][:8]
                    _append({
                        "doi": raw,
                        "title": (item.get("title") or "")[:500],
                        "abstract": (item.get("abstractText") or "")[:2000],
                        "authors": auths, "year": item.get("pubYear"),
                        "journal": (item.get("journalTitle") or "")[:200],
                        "cited_by": item.get("citedByCount", 0),
                        "concepts": [],
                        "is_oa": item.get("isOpenAccess","N") == "Y",
                        "oa_url": "", "ref_count": 0, "language": "",
                        "source": "europepmc",
                    }, disc, sub)
                    new += 1
                next_c = data.get("nextCursorMark","")
                if not next_c or next_c == cursor: _mark_done(key); return new
                cursor = next_c; break
            except: await asyncio.sleep(10)
        await asyncio.sleep(0.3)
    _mark_done(key); return new

# ── Semantic Scholar ─────────────────────────────────────────
async def fetch_s2(client, query, disc, sub):
    if not _time_ok(): return 0
    key = f"S2::{disc}::{sub}::{query[:40]}"
    if _progress.get(key): return 0
    new, offset = 0, 0
    fields = "externalIds,title,abstract,year,authors,citationCount,openAccessPdf,publicationVenue"
    while offset <= 900 and _time_ok():
        url = (f"https://api.semanticscholar.org/graph/v1/paper/search"
               f"?query={query.replace(' ','+')}&offset={offset}&limit=100&fields={fields}")
        for _ in range(3):
            try:
                r = await client.get(url, timeout=30)
                if r.status_code == 429: await asyncio.sleep(60); continue
                if r.status_code != 200: _mark_done(key); return new
                papers = r.json().get("data",[])
                if not papers: _mark_done(key); return new
                for paper in papers:
                    ext = paper.get("externalIds") or {}
                    raw = (ext.get("DOI") or "").strip()
                    if not raw: continue
                    venue  = paper.get("publicationVenue") or {}
                    oa_pdf = paper.get("openAccessPdf") or {}
                    _append({
                        "doi": raw,
                        "title": (paper.get("title") or "")[:500],
                        "abstract": (paper.get("abstract") or "")[:2000],
                        "authors": [a.get("name","") for a in (paper.get("authors") or [])][:8],
                        "year": paper.get("year"),
                        "journal": (venue.get("name") or "")[:200],
                        "cited_by": paper.get("citationCount", 0),
                        "concepts": [],
                        "is_oa": bool(oa_pdf.get("url")),
                        "oa_url": oa_pdf.get("url",""),
                        "ref_count": 0, "language": "", "source": "s2",
                    }, disc, sub)
                    new += 1
                break
            except: await asyncio.sleep(15)
        offset += 100
        await asyncio.sleep(1.0)
    _mark_done(key); return new

# ── DOAJ ─────────────────────────────────────────────────────
async def fetch_doaj(client, query, disc, sub):
    if not _time_ok(): return 0
    key = f"DOAJ::{disc}::{sub}::{query[:40]}"
    if _progress.get(key): return 0
    new, page = 0, 1
    while page <= 10 and _time_ok():
        url = (f"https://doaj.org/api/search/articles/{query.replace(' ','%20')}"
               f"?page={page}&pageSize=100")
        for _ in range(3):
            try:
                r = await client.get(url, timeout=30)
                if r.status_code == 429: await asyncio.sleep(20); continue
                if r.status_code != 200: _mark_done(key); return new
                results = r.json().get("results",[])
                if not results: _mark_done(key); return new
                for item in results:
                    bib = item.get("bibjson",{})
                    raw = next((id_["id"] for id_ in bib.get("identifier",[])
                                if id_.get("type") == "doi"), "").strip()
                    if not raw: continue
                    _append({
                        "doi": raw,
                        "title": (bib.get("title") or "")[:500],
                        "abstract": (bib.get("abstract") or "")[:2000],
                        "authors": [a.get("name","") for a in bib.get("author",[])[:8]],
                        "year": bib.get("year"),
                        "journal": (bib.get("journal",{}).get("title") or "")[:200],
                        "cited_by": 0, "concepts": [],
                        "is_oa": True, "oa_url": "",
                        "ref_count": 0, "language": "", "source": "doaj",
                    }, disc, sub)
                    new += 1
                break
            except: await asyncio.sleep(10)
        page += 1
        await asyncio.sleep(0.3)
    _mark_done(key); return new

# ── Ana döngü ────────────────────────────────────────────────
FETCH_MAP = {
    "oa":   lambda c,d,s,q: fetch_oa(c,q,d,s),
    "cr":   lambda c,d,s,q: fetch_cr(c,q,d,s),
    "epmc": lambda c,d,s,q: fetch_epmc(c,q,d,s),
    "s2":   lambda c,d,s,q: fetch_s2(c,q,d,s),
    "doaj": lambda c,d,s,q: fetch_doaj(c,q,d,s),
}
TIMEOUTS = {"oa":150,"cr":120,"epmc":120,"s2":180,"doaj":90}

async def run():
    _load()
    queries = load_queries()

    sources = ["oa","cr","epmc","doaj","s2"]
    all_tasks = []
    for src in sources:
        prefix = src.upper() + "::"
        for d,s,q in queries:
            key = f"{src.upper()}::{d}::{s}::{q[:40]}"
            if not _progress.get(key):
                all_tasks.append((src, d, s, q))

    total_pending = len(all_tasks)
    log.info(f"Job {JOB_ID}: {len(queries)} query × {len(sources)} kaynak = {total_pending} bekleyen fetch")

    async with httpx.AsyncClient(follow_redirects=True) as client:
        BATCH = 60
        for i in range(0, len(all_tasks), BATCH):
            if not _time_ok():
                log.info("Zaman doldu, duruyorum")
                break
            batch = all_tasks[i:i+BATCH]
            coros = [asyncio.wait_for(FETCH_MAP[src](client,d,s,q),
                     timeout=TIMEOUTS[src])
                     for src,d,s,q in batch]
            results = await asyncio.gather(*coros, return_exceptions=True)
            batch_new = sum(r for r in results if isinstance(r,int))
            log.info(f"Batch {i//BATCH+1}: +{batch_new} | toplam: {len(_seen):,}")
            _save_progress()
            _save_summary()

    if _master_f: _master_f.close()
    _save_progress()
    _save_summary()
    log.info(f"BITTI — bu run: +{_new_count:,} | toplam: {len(_seen):,}")

if __name__ == "__main__":
    asyncio.run(run())
