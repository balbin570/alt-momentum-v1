import requests,datetime,statistics,json
from collections import defaultdict
u="https://alt-momentum-v1.onrender.com/paper-records"
rows=[]; offset=0
while True:
 r=requests.get(u,params={"limit":500,"offset":offset},timeout=100);r.raise_for_status()
 d=r.json();part=d["trades"];rows+=part;offset+=len(part)
 if not part or offset>=d["total"]:break
start=datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(hours=24)
models=["C","C_EARLY_EXIT_5M_NEG05","C_EARLY_EXIT_10M_NEG"]
print("REPORT_UTC",datetime.datetime.now(datetime.timezone.utc).isoformat(),"WINDOW_START",start.isoformat(),"TOTAL_RECORDS",len(rows),flush=True)
for v in models:
 ts=[t for t in rows if t.get("variant")==v and t.get("entry_utc") and datetime.datetime.fromisoformat(t["entry_utc"].replace("Z","+00:00"))>=start]
 closed=[t for t in ts if t.get("status")=="CLOSED_SHADOW" and isinstance(t.get("net_pct"),(int,float))]
 vals=[t["net_pct"] for t in closed]; wins=[x for x in vals if x>0];losses=[x for x in vals if x<0]
 early=[t for t in closed if str(t.get("exit_reason","")).startswith("EARLY_")]
 print(json.dumps({"model":v,"entries_24h":len(ts),"closed_of_entries":len(closed),"open":len(ts)-len(closed),"wins":len(wins),"win_rate_pct":round(len(wins)/len(vals)*100,2) if vals else None,"mean_net_pct":round(statistics.mean(vals),5) if vals else None,"sum_net_pct":round(sum(vals),5),"profit_factor":round(sum(wins)/-sum(losses),4) if losses else None,"early_exits":len(early),"recent": [{"symbol":t.get("symbol"),"net_pct":t["net_pct"],"reason":t.get("exit_reason")} for t in closed[-6:]]},ensure_ascii=False),flush=True)
