import csv,io,json,time,urllib.request,zipfile
from datetime import datetime,timedelta,timezone
from pathlib import Path
from scipy.stats import fisher_exact,spearmanr
BASE="https://alt-momentum-v1.onrender.com/paper-records"
def fetch(url):
 for i in range(5):
  try:
   with urllib.request.urlopen(urllib.request.Request(url,headers={"User-Agent":"C-research"}),timeout=90) as r:return r.read()
  except Exception:
   if i==4:raise
   time.sleep(5*(i+1))
trades=[]
for offset in range(0,10000,500):
 data=json.loads(fetch(f"{BASE}?limit=500&offset={offset}"))
 trades+=data["trades"]
 if offset+500>=data["total"]:break
old=[t for t in trades if t.get("variant")=="C" and not t.get("c_rule_version") and not t.get("c_entry_version") and t.get("status")=="CLOSED_SHADOW" and isinstance(t.get("net_pct"),(int,float))]
old=list({(t["symbol"],t["entry_ms"]):t for t in old}.values())
old.sort(key=lambda t:t["entry_ms"])
assert old,"Legacy C trades missing"
start=datetime.fromtimestamp(old[0]["entry_ms"]/1000,timezone.utc).date()-timedelta(days=1)
end=datetime.fromtimestamp(old[-1]["entry_ms"]/1000,timezone.utc).date()
days=[start+timedelta(days=i) for i in range((end-start).days+1)]
prices={}
for sym in ("BTCUSDT","ETHUSDT","BNBUSDT"):
 p={}
 for day in days:
  d=day.isoformat()
  url=f"https://data.binance.vision/data/spot/daily/klines/{sym}/1m/{sym}-1m-{d}.zip"
  with zipfile.ZipFile(io.BytesIO(fetch(url))) as z:
   with z.open(next(n for n in z.namelist() if n.endswith(".csv"))) as f:
    for row in csv.reader(io.TextIOWrapper(f)):
     try:ts=int(row[0]);close=float(row[4])
     except (ValueError,IndexError):continue
     ms=ts//1000 if ts>10**14 else ts
     p[ms//60000]=close
  print(sym,d,len(p),flush=True)
 prices[sym]=p
rows=[]
for t in old:
 minute=t["entry_ms"]//60000
 r={"symbol":t["symbol"],"entry_utc":t["entry_utc"],"net_pct":t["net_pct"],"win":int(t["net_pct"]>0)}
 for sym,p in prices.items():
  for n in (5,15,60):
   a=p.get(minute-1);b=p.get(minute-1-n)
   r[f"{sym[:3]}_{n}m"]=100*(a/b-1) if a and b else None
 rows.append(r)
Path("research/results").mkdir(parents=True,exist_ok=True)
with open("research/results/legacy_c_market_join.csv","w",newline="") as f:
 w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
tests=[]
for sym in ("BTC","ETH","BNB"):
 for n in (5,15,60):
  key=f"{sym}_{n}m";sample=[r for r in rows if r[key] is not None]
  up=[r for r in sample if r[key]>=0];down=[r for r in sample if r[key]<0]
  if not up or not down:continue
  a=sum(r["win"] for r in up);b=len(up)-a;c=sum(r["win"] for r in down);d=len(down)-c
  p=fisher_exact([[a,b],[c,d]]).pvalue
  rho,rhop=spearmanr([r[key] for r in sample],[r["net_pct"] for r in sample])
  tests.append({"feature":key,"n":len(sample),"up_n":len(up),"up_win_rate":a/len(up),"up_mean_net":sum(r["net_pct"] for r in up)/len(up),"down_n":len(down),"down_win_rate":c/len(down),"down_mean_net":sum(r["net_pct"] for r in down)/len(down),"fisher_p":p,"spearman_rho":rho,"spearman_p":rhop})
# BH correction over 18 tests (9 features, 2 tests each).
ordered=sorted([(t[k],i,k) for i,t in enumerate(tests) for k in ("fisher_p","spearman_p")])
q=1.
for rank in range(len(ordered),0,-1):
 p,i,k=ordered[rank-1];q=min(q,p*len(ordered)/rank);tests[i][k+"_bh_q"]=q
report={"legacy_c_closed":len(old),"start":old[0]["entry_utc"],"end":old[-1]["entry_utc"],"tests":tests,"caution":"Exploratory in-sample correlations; no changes to trading rules."}
Path("research/results/legacy_c_market_report.json").write_text(json.dumps(report,indent=2))
print(json.dumps(report,indent=2),flush=True)
