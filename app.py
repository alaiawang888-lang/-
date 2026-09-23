import io, os, re, sqlite3, zipfile, hashlib, asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from pypdf import PdfReader

BASE = Path(__file__).parent
DB = BASE / "tracker.db"
STATIC = BASE / "static"
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "300"))
YEAR = int(os.getenv("DISCLOSURE_YEAR", str(datetime.now().year)))
HOUSE_ZIP = f"https://disclosures-clerk.house.gov/public_disc/financial-pdfs/{YEAR}FD.zip"
HOUSE_DOC = "https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/{year}/{doc}.pdf"
OGE_SEARCH = "https://www.oge.gov/web/OGE.nsf/Officials%20Individual%20Disclosures%20Search%20Collection?OpenForm"

app = FastAPI(title="Capitol Tracker")
app.mount("/static", StaticFiles(directory=STATIC), name="static")


def conn():
    c=sqlite3.connect(DB); c.row_factory=sqlite3.Row; return c

def init_db():
    with conn() as c:
        c.executescript('''
        CREATE TABLE IF NOT EXISTS filings(
          id INTEGER PRIMARY KEY, source TEXT, doc_id TEXT UNIQUE, person TEXT,
          filing_type TEXT, filing_date TEXT, pdf_url TEXT, detected_at TEXT,
          parse_status TEXT DEFAULT 'pending'
        );
        CREATE TABLE IF NOT EXISTS trades(
          id INTEGER PRIMARY KEY, filing_id INTEGER, person TEXT, owner TEXT,
          asset TEXT, ticker TEXT, tx_type TEXT, tx_date TEXT, amount TEXT,
          source TEXT, pdf_url TEXT, detected_at TEXT,
          large INTEGER DEFAULT 0, repeat_buy INTEGER DEFAULT 0,
          UNIQUE(filing_id, asset, tx_type, tx_date, amount)
        );
        CREATE TABLE IF NOT EXISTS source_status(
          source TEXT PRIMARY KEY, last_check TEXT, ok INTEGER, detail TEXT
        );
        ''')


def amount_upper(s):
    nums=[]
    for x in re.findall(r'\$?([\d,]+)', s or ''):
        try: nums.append(int(x.replace(',','')))
        except: pass
    return max(nums) if nums else 0

def ticker_guess(asset):
    m=re.search(r'\(([A-Z]{1,6})\)', asset or '')
    return m.group(1) if m else ''

def parse_house_zip(data: bytes):
    z=zipfile.ZipFile(io.BytesIO(data))
    txts=[n for n in z.namelist() if n.lower().endswith('.txt')]
    if not txts: return []
    raw=z.read(txts[0]).decode('utf-8','ignore')
    rows=[]
    for line in raw.splitlines()[1:]:
        parts=line.split('\t')
        if len(parts)<8: continue
        # Clerk index format: Prefix, Last, First, Suffix, FilingType, State/District, Year, FilingDate, DocID
        doc=parts[-1].strip(); filing_date=parts[-2].strip(); filing_type=parts[4].strip() if len(parts)>4 else ''
        if filing_type.upper() not in {'P','PTR'}: continue
        person=' '.join(x for x in [parts[2].strip(),parts[1].strip()] if x)
        rows.append((doc, person, filing_type, filing_date))
    return rows

def pdf_text(data: bytes):
    try:
        r=PdfReader(io.BytesIO(data)); return '\n'.join((p.extract_text() or '') for p in r.pages)
    except Exception: return ''

def parse_ptr_text(text, person, source, url, filing_id):
    # Conservative parser: only emits rows where type/date/amount are visible together.
    lines=[' '.join(x.split()) for x in text.splitlines() if x.strip()]
    out=[]
    pattern=re.compile(r'(?P<asset>.+?)\s+(?P<type>Purchase|Sale|Exchange)\s+(?P<date>\d{1,2}/\d{1,2}/\d{4})\s+(?P<amount>\$?[\d,]+\s*-\s*\$?[\d,]+)', re.I)
    for line in lines:
        m=pattern.search(line)
        if not m: continue
        asset=m.group('asset')[-180:]; typ=m.group('type').title(); amount=m.group('amount'); txd=m.group('date')
        owner='Spouse' if re.search(r'\bSP\b|spouse', line, re.I) else ('Dependent' if re.search(r'\bDC\b|dependent',line,re.I) else 'Unknown')
        out.append(dict(filing_id=filing_id,person=person,owner=owner,asset=asset,ticker=ticker_guess(asset),tx_type=typ,tx_date=txd,amount=amount,source=source,pdf_url=url))
    return out

async def poll_house():
    now=datetime.now(timezone.utc).isoformat()
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True, headers={'User-Agent':'CapitolTracker/0.1 public-disclosure-monitor'}) as h:
            rz=await h.get(HOUSE_ZIP); rz.raise_for_status(); rows=parse_house_zip(rz.content)
            new=[]
            with conn() as c:
                for doc,person,ft,fd in rows:
                    url=HOUSE_DOC.format(year=YEAR,doc=doc)
                    cur=c.execute("INSERT OR IGNORE INTO filings(source,doc_id,person,filing_type,filing_date,pdf_url,detected_at) VALUES(?,?,?,?,?,?,?)",('House',doc,person,ft,fd,url,now))
                    if cur.rowcount: new.append((cur.lastrowid,doc,person,url))
                c.execute("INSERT OR REPLACE INTO source_status VALUES(?,?,?,?)",('House',now,1,f'{len(rows)} PTR index rows; {len(new)} new'))
            for fid,doc,person,url in new[:100]:
                try:
                    rp=await h.get(url); rp.raise_for_status(); text=pdf_text(rp.content); trades=parse_ptr_text(text,person,'House',url,fid)
                    with conn() as c:
                        for t in trades:
                            large=1 if amount_upper(t['amount'])>=500000 else 0
                            repeat=0
                            if t['tx_type']=='Purchase' and (t['ticker'] or t['asset']):
                                key=t['ticker'] or t['asset']
                                q="SELECT 1 FROM trades WHERE person=? AND tx_type='Purchase' AND (ticker=? OR asset=?) LIMIT 1"
                                repeat=1 if c.execute(q,(person,key,key)).fetchone() else 0
                            c.execute('''INSERT OR IGNORE INTO trades(filing_id,person,owner,asset,ticker,tx_type,tx_date,amount,source,pdf_url,detected_at,large,repeat_buy)
                              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)''',(fid,person,t['owner'],t['asset'],t['ticker'],t['tx_type'],t['tx_date'],t['amount'],'House',url,now,large,repeat))
                        c.execute("UPDATE filings SET parse_status=? WHERE id=?",('parsed' if trades else 'needs_review',fid))
                except Exception:
                    with conn() as c: c.execute("UPDATE filings SET parse_status='fetch_error' WHERE id=?",(fid,))
    except Exception as e:
        with conn() as c: c.execute("INSERT OR REPLACE INTO source_status VALUES(?,?,?,?)",('House',now,0,str(e)[:300]))

async def poll_oge_status():
    # OGE PTRs are subject to special access rules; this watcher verifies the official search endpoint is reachable.
    now=datetime.now(timezone.utc).isoformat()
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as h:
            r=await h.get(OGE_SEARCH); ok=1 if r.status_code<400 else 0
        detail='Official OGE disclosure search reachable; individual PTR acquisition may require Form 201/request flow.'
    except Exception as e: ok=0; detail=str(e)[:300]
    with conn() as c: c.execute("INSERT OR REPLACE INTO source_status VALUES(?,?,?,?)",('OGE',now,ok,detail))

async def poll_all():
    await poll_house(); await poll_oge_status()

@app.on_event("startup")
async def startup():
    init_db()
    sched=AsyncIOScheduler(); sched.add_job(poll_all,'interval',seconds=POLL_SECONDS,max_instances=1,coalesce=True); sched.start(); app.state.sched=sched
    asyncio.create_task(poll_all())

@app.get('/api/trades')
def trades(person:Optional[str]=None, kind:Optional[str]=None, limit:int=Query(100,le=500)):
    sql='SELECT * FROM trades WHERE 1=1'; args=[]
    if person: sql+=' AND person LIKE ?'; args.append('%'+person+'%')
    if kind=='large': sql+=' AND large=1'
    if kind=='repeat': sql+=' AND repeat_buy=1'
    sql+=' ORDER BY detected_at DESC, id DESC LIMIT ?'; args.append(limit)
    with conn() as c: return [dict(x) for x in c.execute(sql,args)]

@app.get('/api/filings')
def filings(limit:int=100):
    with conn() as c: return [dict(x) for x in c.execute('SELECT * FROM filings ORDER BY detected_at DESC,id DESC LIMIT ?',(limit,))]

@app.get('/api/status')
def status():
    with conn() as c: return {'poll_seconds':POLL_SECONDS,'year':YEAR,'sources':[dict(x) for x in c.execute('SELECT * FROM source_status')]}

@app.get('/')
def index(): return FileResponse(STATIC/'index.html')
@app.get('/manifest.webmanifest')
def manifest(): return FileResponse(STATIC/'manifest.webmanifest',media_type='application/manifest+json')
@app.get('/sw.js')
def sw(): return FileResponse(STATIC/'sw.js',media_type='application/javascript')
