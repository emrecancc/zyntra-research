"""
doi_collector_github.py — GitHub Actions checkpoint'li DOI toplayici.
- Her 30 dakikada bir calisir (workflow_dispatch veya cron)
- 25 dakika calistiktan sonra timeout ile kesilir
- Checkpoint'li: kaldigi yerden devam eder
- Her 100 DOI'de summary.json gunceller (commit icin)
"""
import asyncio, json, time, logging, os, signal, sys
from pathlib import Path
import httpx

DATA_DIR    = Path("data")
MASTER_FILE = DATA_DIR / "_master.jsonl"
PROGRESS    = DATA_DIR / "progress.json"
SUMMARY     = DATA_DIR / "summary.json"
LOG_FILE    = DATA_DIR / "collector.log"
DATA_DIR.mkdir(exist_ok=True)

OA_EMAIL   = "emrecancerli55@gmail.com"
OA_HEADERS = {"User-Agent": f"ZyntraResearch/3.0 (mailto:{OA_EMAIL})"}
PER_PAGE   = 200
MAX_PAGES  = 25   # her query 5000 DOI max — daha fazla query'e zaman kalsin

# 23 dakika sonra dur (workflow 28dk timeout, bize 5dk commit icin kalir)
HARD_STOP  = time.time() + 23 * 60
_shutdown  = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("doi")

# ── Taxonomy ─────────────────────────────────────────────────
def load_queries():
    tax_file = Path("Rocket/research_taxonomy.json")
    if not tax_file.exists():
        log.error("Taxonomy bulunamadi!")
        return []
    tax = json.loads(tax_file.read_text(encoding="utf-8-sig"))
    queries = []
    for disc_key, disc in tax.get("disciplines", {}).items():
        if disc.get("deferred_fulltext"): continue
        for sub_key, sub in disc.get("subtopics", {}).items():
            if "papers" in sub.get("sources", []):
                for q in sub.get("queries", []):
                    queries.append((disc_key, sub_key, q))
    return queries

# ── Store ────────────────────────────────────────────────────
_seen = set()
_progress = {}
_master_f = None
_new_this_run = 0

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
    global _master_f, _new_this_run
    doi = doi_obj.get("doi", "").strip()
    if not doi or doi in _seen: return False
    _seen.add(doi)
    rec = json.dumps({**doi_obj, "disc": disc, "sub": sub, "ts": int(time.time())}, ensure_ascii=False)
    if _master_f is None:
        _master_f = open(MASTER_FILE, "a", encoding="utf-8")
    _master_f.write(rec + "\n")
    _master_f.flush()
    _new_this_run += 1
    # Her 100 DOI'de summary guncelle
    if _new_this_run % 100 == 0:
        _save_summary()
    return True

def _save_progress():
    PROGRESS.write_text(json.dumps(_progress))

def _mark_done(key):
    _progress[key] = True
    if sum(1 for v in _progress.values() if v) % 10 == 0:
        _save_progress()

def _save_summary():
    SUMMARY.write_text(json.dumps({
        "run_time": time.strftime("%Y-%m-%d %H:%M UTC"),
        "total_doi": len(_seen),
        "new_this_run": _new_this_run,
        "queries_done": sum(1 for v in _progress.values() if v),
        "queries_total": len(_progress),
    }, indent=2))

def _time_ok():
    return time.time() < HARD_STOP and not _shutdown

# ── OpenAlex ─────────────────────────────────────────────────
async def fetch_oa(client, query, disc, sub):
    if not _time_ok(): return 0
    key = f"OA::{disc}::{sub}::{query[:40]}"
    if _progress.get(key): return 0
    cursor, new = "*", 0
    for page in range(MAX_PAGES):
        if not _time_ok(): break
        url = (f"https://api.openalex.org/works"
               f"?search={query.replace(' ','+')}&per-page={PER_PAGE}"
               f"&cursor={cursor}&select=doi,title,abstract_inverted_index,"
               f"authorships,publication_year,cited_by_count,concepts,"
               f"primary_location,open_access,referenced_works_count,language")
        for attempt in range(3):
            try:
                r = await client.get(url, headers=OA_HEADERS, timeout=30)
                if r.status_code == 429:
                    await asyncio.sleep(30); continue
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
                    _append({
                        "doi": raw, "title": (w.get("title") or "")[:500],
                        "abstract": abstract[:2000], "authors": auths,
                        "year": w.get("publication_year"),
                        "journal": (src.get("display_name") or "")[:200],
                        "cited_by": w.get("cited_by_count", 0), "concepts": concepts,
                        "is_oa": oa.get("is_oa", False), "oa_url": oa.get("oa_url", ""),
                        "ref_count": w.get("referenced_works_count", 0),
                        "language": w.get("language", ""), "source": "openalex",
                    }, disc, sub)
                    new += 1
                cursor = data.get("meta", {}).get("next_cursor", "")
                if not cursor: _mark_done(key); return new
                break
            except Exception as e:
                await asyncio.sleep(10)
        await asyncio.sleep(0.3)
    _mark_done(key)
    return new

# ── Crossref ─────────────────────────────────────────────────
async def fetch_cr(client, query, disc, sub):
    if not _time_ok(): return 0
    key = f"CR::{disc}::{sub}::{query[:40]}"
    if _progress.get(key): return 0
    new, offset = 0, 0
    while offset <= 4000 and _time_ok():
        url = (f"https://api.crossref.org/works"
               f"?query={query.replace(' ','+')}&rows=200&offset={offset}&mailto={OA_EMAIL}")
        for attempt in range(3):
            try:
                r = await client.get(url, timeout=30)
                if r.status_code == 429: await asyncio.sleep(20); continue
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
                    _append({
                        "doi": raw, "title": (titles[0] if titles else "")[:500],
                        "abstract": (item.get("abstract") or "")[:2000], "authors": auths,
                        "year": issued[0] if issued else None,
                        "journal": (item.get("container-title") or [""])[0][:200],
                        "cited_by": item.get("is-referenced-by-count", 0),
                        "concepts": [], "is_oa": False, "oa_url": "",
                        "ref_count": item.get("references-count", 0),
                        "language": "", "source": "crossref",
                    }, disc, sub)
                    new += 1
                break
            except Exception as e:
                await asyncio.sleep(10)
        offset += 200
        await asyncio.sleep(0.2)
    _mark_done(key)
    return new

# ── EuropePMC ─────────────────────────────────────────────────
async def fetch_epmc(client, query, disc, sub):
    if not _time_ok(): return 0
    key = f"EPMC::{disc}::{sub}::{query[:40]}"
    if _progress.get(key): return 0
    new, cursor = 0, "*"
    for page in range(15):
        if not _time_ok(): break
        url = (f"https://www.ebi.ac.uk/europepmc/webservices/rest/search"
               f"?query={query.replace(' ','+')}&format=json&pageSize=200"
               f"&cursorMark={cursor}&resultType=core&sort=CITED+desc")
        for attempt in range(3):
            try:
                r = await client.get(url, timeout=30)
                if r.status_code == 429: await asyncio.sleep(20); continue
                if r.status_code != 200: _mark_done(key); return new
                data = r.json()
                results = data.get("resultList", {}).get("result", [])
                if not results: _mark_done(key); return new
                for item in results:
                    raw = (item.get("doi") or "").strip()
                    if not raw: continue
                    auths = [a.get("fullName", "") for a in
                             (item.get("authorList", {}).get("author") or [])][:8]
                    _append({
                        "doi": raw, "title": (item.get("title") or "")[:500],
                        "abstract": (item.get("abstractText") or "")[:2000],
                        "authors": auths, "year": item.get("pubYear"),
                        "journal": (item.get("journalTitle") or "")[:200],
                        "cited_by": item.get("citedByCount", 0),
                        "concepts": [], "is_oa": item.get("isOpenAccess", "N") == "Y",
                        "oa_url": "", "ref_count": 0, "language": "", "source": "europepmc",
                    }, disc, sub)
                    new += 1
                next_c = data.get("nextCursorMark", "")
                if not next_c or next_c == cursor: _mark_done(key); return new
                cursor = next_c
                break
            except Exception as e:
                await asyncio.sleep(10)
        await asyncio.sleep(0.3)
    _mark_done(key)
    return new

# ── Semantic Scholar ──────────────────────────────────────────
async def fetch_s2(client, query, disc, sub):
    if not _time_ok(): return 0
    key = f"S2::{disc}::{sub}::{query[:40]}"
    if _progress.get(key): return 0
    new, offset = 0, 0
    fields = "externalIds,title,abstract,year,authors,citationCount,openAccessPdf,publicationVenue"
    while offset <= 900 and _time_ok():
        url = (f"https://api.semanticscholar.org/graph/v1/paper/search"
               f"?query={query.replace(' ','+')}&offset={offset}&limit=100&fields={fields}")
        for attempt in range(3):
            try:
                r = await client.get(url, timeout=30)
                if r.status_code == 429: await asyncio.sleep(60); continue
                if r.status_code != 200: _mark_done(key); return new
                papers = r.json().get("data", [])
                if not papers: _mark_done(key); return new
                for paper in papers:
                    ext = paper.get("externalIds") or {}
                    raw = (ext.get("DOI") or "").strip()
                    if not raw: continue
                    venue = paper.get("publicationVenue") or {}
                    oa_pdf = paper.get("openAccessPdf") or {}
                    _append({
                        "doi": raw, "title": (paper.get("title") or "")[:500],
                        "abstract": (paper.get("abstract") or "")[:2000],
                        "authors": [a.get("name","") for a in (paper.get("authors") or [])][:8],
                        "year": paper.get("year"),
                        "journal": (venue.get("name") or "")[:200],
                        "cited_by": paper.get("citationCount", 0),
                        "concepts": [], "is_oa": bool(oa_pdf.get("url")),
                        "oa_url": oa_pdf.get("url", ""),
                        "ref_count": 0, "language": "", "source": "s2",
                    }, disc, sub)
                    new += 1
                break
            except Exception as e:
                await asyncio.sleep(15)
        offset += 100
        await asyncio.sleep(1.0)
    _mark_done(key)
    return new

# ── DOAJ ─────────────────────────────────────────────────────
async def fetch_doaj(client, query, disc, sub):
    if not _time_ok(): return 0
    key = f"DOAJ::{disc}::{sub}::{query[:40]}"
    if _progress.get(key): return 0
    new, page = 0, 1
    while page <= 20 and _time_ok():
        url = (f"https://doaj.org/api/search/articles/{query.replace(' ','%20')}"
               f"?page={page}&pageSize=100")
        for attempt in range(3):
            try:
                r = await client.get(url, timeout=30)
                if r.status_code == 429: await asyncio.sleep(20); continue
                if r.status_code != 200: _mark_done(key); return new
                data = r.json()
                results = data.get("results", [])
                if not results: _mark_done(key); return new
                for item in results:
                    bib = item.get("bibjson", {})
                    idents = bib.get("identifier", [])
                    raw = ""
                    for id_ in idents:
                        if id_.get("type") == "doi":
                            raw = id_.get("id", "").strip()
                            break
                    if not raw: continue
                    auths = [a.get("name","") for a in bib.get("author", [])][:8]
                    _append({
                        "doi": raw, "title": (bib.get("title") or "")[:500],
                        "abstract": (bib.get("abstract") or "")[:2000],
                        "authors": auths, "year": (bib.get("year") or None),
                        "journal": (bib.get("journal", {}).get("title") or "")[:200],
                        "cited_by": 0, "concepts": [],
                        "is_oa": True, "oa_url": "",
                        "ref_count": 0, "language": "", "source": "doaj",
                    }, disc, sub)
                    new += 1
                total_pages = data.get("total", 0) // 100 + 1
                if page >= total_pages: _mark_done(key); return new
                break
            except Exception as e:
                await asyncio.sleep(10)
        page += 1
        await asyncio.sleep(0.3)
    _mark_done(key)
    return new

# ── PubMed ───────────────────────────────────────────────────
async def fetch_pubmed(client, query, disc, sub):
    if not _time_ok(): return 0
    key = f"PM::{disc}::{sub}::{query[:40]}"
    if _progress.get(key): return 0
    new = 0
    # Adim 1: PMIDs al
    search_url = (f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
                  f"?db=pubmed&term={query.replace(' ','+')}+AND+free+full+text[filter]"
                  f"&retmax=500&retmode=json&tool=ZyntraResearch&email={OA_EMAIL}")
    try:
        r = await client.get(search_url, timeout=30)
        if r.status_code != 200: _mark_done(key); return 0
        pmids = r.json().get("esearchresult", {}).get("idlist", [])
        if not pmids: _mark_done(key); return 0
    except:
        _mark_done(key); return 0

    # Adim 2: Her 100 PMID icin DOI al
    for i in range(0, len(pmids), 100):
        if not _time_ok(): break
        batch = pmids[i:i+100]
        fetch_url = (f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
                     f"?db=pubmed&id={','.join(batch)}&retmode=json"
                     f"&tool=ZyntraResearch&email={OA_EMAIL}")
        try:
            r = await client.get(fetch_url, timeout=30)
            if r.status_code != 200: continue
            result = r.json().get("result", {})
            for pmid in batch:
                item = result.get(pmid, {})
                eloc = item.get("elocationid", "")
                raw = ""
                if "doi:" in eloc.lower():
                    raw = eloc.lower().replace("doi: ","").replace("doi:","").strip()
                if not raw:
                    for art in item.get("articleids", []):
                        if art.get("idtype") == "doi":
                            raw = art.get("value","").strip()
                            break
                if not raw: continue
                auths = [a.get("name","") for a in item.get("authors",[])[:8]]
                _append({
                    "doi": raw,
                    "title": (item.get("title") or "")[:500],
                    "abstract": "", "authors": auths,
                    "year": (item.get("pubdate","")[:4] or None),
                    "journal": (item.get("fulljournalname") or "")[:200],
                    "cited_by": 0, "concepts": [],
                    "is_oa": True, "oa_url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
                    "ref_count": 0, "language": "", "source": "pubmed",
                }, disc, sub)
                new += 1
        except: pass
        await asyncio.sleep(0.4)

    _mark_done(key)
    return new

# ── Ana döngü: BATCH'Lİ ──────────────────────────────────────
async def run():
    _load()
    queries = load_queries()
    total_q = len(queries)

    pending_oa   = [(d,s,q) for d,s,q in queries if not _progress.get(f"OA::{d}::{s}::{q[:40]}")]
    pending_cr   = [(d,s,q) for d,s,q in queries if not _progress.get(f"CR::{d}::{s}::{q[:40]}")]
    pending_epmc = [(d,s,q) for d,s,q in queries if not _progress.get(f"EPMC::{d}::{s}::{q[:40]}")]
    pending_s2   = [(d,s,q) for d,s,q in queries if not _progress.get(f"S2::{d}::{s}::{q[:40]}")]
    pending_doaj = [(d,s,q) for d,s,q in queries if not _progress.get(f"DOAJ::{d}::{s}::{q[:40]}")]
    pending_pm   = [(d,s,q) for d,s,q in queries if not _progress.get(f"PM::{d}::{s}::{q[:40]}")]

    log.info(f"{'='*60}")
    log.info(f"DOI Collector v2 — {total_q} sorgu, 6 kaynak")
    log.info(f"OA={len(pending_oa)} CR={len(pending_cr)} EPMC={len(pending_epmc)} S2={len(pending_s2)} DOAJ={len(pending_doaj)} PM={len(pending_pm)}")
    log.info(f"Mevcut DOI: {len(_seen):,} | Kalan sure: {int(HARD_STOP-time.time()):}s")
    log.info(f"{'='*60}")

    # Tum kaynaklari interleave et — bir query'yi 6 kaynaktan ayni anda cek
    # Siralama: OA en verimli, once o, sonra diger kaynaklar
    all_tasks = []
    # Once OA (en fazla DOI getiriyor)
    for d,s,q in pending_oa:   all_tasks.append(("oa",   d, s, q))
    # Sonra CR + EPMC (hizli)
    for d,s,q in pending_cr:   all_tasks.append(("cr",   d, s, q))
    for d,s,q in pending_epmc: all_tasks.append(("epmc", d, s, q))
    # Sonra DOAJ + PubMed
    for d,s,q in pending_doaj: all_tasks.append(("doaj", d, s, q))
    for d,s,q in pending_pm:   all_tasks.append(("pm",   d, s, q))
    # S2 en sonda (rate limit agir)
    for d,s,q in pending_s2:   all_tasks.append(("s2",   d, s, q))

    FETCH_MAP = {
        "oa":   lambda c,d,s,q: fetch_oa(c,q,d,s),
        "cr":   lambda c,d,s,q: fetch_cr(c,q,d,s),
        "epmc": lambda c,d,s,q: fetch_epmc(c,q,d,s),
        "doaj": lambda c,d,s,q: fetch_doaj(c,q,d,s),
        "pm":   lambda c,d,s,q: fetch_pubmed(c,q,d,s),
        "s2":   lambda c,d,s,q: fetch_s2(c,q,d,s),
    }
    TIMEOUTS = {"oa":150, "cr":120, "epmc":120, "doaj":90, "pm":120, "s2":180}

    async with httpx.AsyncClient(follow_redirects=True) as client:
        # Batch boyutu 80 — daha fazla paralel istek
        BATCH = 80
        for i in range(0, len(all_tasks), BATCH):
            if not _time_ok():
                log.info("Zaman doldu, duruyorum...")
                break
            batch = all_tasks[i:i+BATCH]
            coros = []
            for src, d, s, q in batch:
                fn = FETCH_MAP[src]
                coros.append(asyncio.wait_for(
                    fn(client, d, s, q),
                    timeout=TIMEOUTS[src]))

            results = await asyncio.gather(*coros, return_exceptions=True)
            batch_new = sum(r for r in results if isinstance(r, int))
            log.info(f"Batch {i//BATCH+1}/{(len(all_tasks)+BATCH-1)//BATCH}: "
                     f"+{batch_new} DOI | toplam: {len(_seen):,}")
            _save_progress()
            _save_summary()

    if _master_f:
        _master_f.close()
    _save_progress()
    _save_summary()

    log.info(f"TAMAMLANDI — Bu run: +{_new_this_run:,} | Toplam: {len(_seen):,}")

if __name__ == "__main__":
    asyncio.run(run())
