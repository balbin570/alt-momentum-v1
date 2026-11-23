import requests,datetime,statistics,json
from collections import defaultdict
u="https://alt-momentum-v1.onrender.com/paper-records"
rows=[]; offset=0
while True:
 r=requests.get(u,params={"limit":500,"offset":offset},timeout=100);r.raise_for_status()
 d=r.json();part=d["trades"];rows+=part;offset+=len(part)
 if not part or offset>=d["total"]:break
start=datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(hours=24)
models=["C","C_EARLY_EXIT_5M_NEG05","C_EARLY_EXIT_10M_NEG","C_5M_MORE_ENTRIES_V1","C_10M_MORE_ENTRIES_V1"]
print("REPORT_UTC",datetime.datetime.now(datetime.timezone.utc).isoformat(),"WINDOW_START",start.isoformat(),"TOTAL_RECORDS",len(rows),flush=True)
for v in models:
 ts=[t for t in rows if t.get("variant")==v and t.get("entry_utc") and datetime.datetime.fromisoformat(t["entry_utc"].replace("Z","+00:00"))>=start]
 closed=[t for t in ts if t.get("status")=="CLOSED_SHADOW" and isinstance(t.get("net_pct"),(int,float))]
 vals=[t["net_pct"] for t in closed]; wins=[x for x in vals if x>0];losses=[x for x in vals if x<0]
 early=[t for t in closed if str(t.get("exit_reason","")).startswith("EARLY_")]
 print(json.dumps({"model":v,"entries_24h":len(ts),"closed_of_entries":len(closed),"open":len(ts)-len(closed),"wins":len(wins),"win_rate_pct":round(len(wins)/len(vals)*100,2) if vals else None,"mean_net_pct":round(statistics.mean(vals),5) if vals else None,"sum_net_pct":round(sum(vals),5),"profit_factor":round(sum(wins)/-sum(losses),4) if losses else None,"early_exits":len(early),"recent": [{"symbol":t.get("symbol"),"net_pct":t["net_pct"],"reason":t.get("exit_reason")} for t in closed[-6:]]},ensure_ascii=False),flush=True)

# Extra audit: all large realized winners and exits within the window,
# including positions entered before the 24-hour cutoff.
print("=== LARGE_WINNERS_ALL_HISTORY ===",flush=True)
for v in models:
 winners=sorted([t for t in rows if t.get("variant")==v and t.get("status")=="CLOSED_SHADOW" and isinstance(t.get("net_pct"),(int,float)) and t["net_pct"]>=5],key=lambda t:t["net_pct"],reverse=True)
 print(json.dumps({"model":v,"large_winners_count":len(winners),"top": [{"symbol":t.get("symbol"),"entry_utc":t.get("entry_utc"),"exit_utc":t.get("exit_utc"),"net_pct":t.get("net_pct"),"reason":t.get("exit_reason")} for t in winners[:30]]}),flush=True)
print("=== CLOSED_IN_LAST_24H ===",flush=True)
for v in models:
 ts=[t for t in rows if t.get("variant")==v and t.get("status")=="CLOSED_SHADOW" and isinstance(t.get("net_pct"),(int,float)) and t.get("exit_utc") and datetime.datetime.fromisoformat(t["exit_utc"].replace("Z","+00:00"))>=start]
 vals=[t["net_pct"] for t in ts]; gains=[x for x in vals if x>0];loss=[x for x in vals if x<0]
 print(json.dumps({"model":v,"closed_24h":len(ts),"wins":len(gains),"win_rate_pct":round(100*len(gains)/len(vals),2) if vals else None,"mean_net_pct":round(statistics.mean(vals),5) if vals else None,"sum_net_pct":round(sum(vals),5),"profit_factor":round(sum(gains)/-sum(loss),4) if loss else None,"best": [{"symbol":t.get("symbol"),"net_pct":t["net_pct"],"exit_utc":t.get("exit_utc")} for t in sorted(ts,key=lambda t:t["net_pct"],reverse=True)[:10]]}),flush=True)

print("=== NEW_COHORTS_24H_AUDIT ===",flush=True)
for v in models:
 cohort=[t for t in rows if t.get("variant")==v and t.get("entry_utc") and datetime.datetime.fromisoformat(t["entry_utc"].replace("Z","+00:00"))>=start]
 closed=[t for t in cohort if t.get("status")=="CLOSED_SHADOW" and isinstance(t.get("net_pct"),(int,float))]
 vals=[t["net_pct"] for t in closed]; wins=[x for x in vals if x>0]; losses=[x for x in vals if x<0]
 print(json.dumps({"model":v,"entries_24h":len(cohort),"closed":len(closed),"open":len(cohort)-len(closed),"wins":len(wins),"win_rate_pct":round(len(wins)*100/len(vals),2) if vals else None,"mean_net_pct":round(statistics.mean(vals),5) if vals else None,"sum_net_pct":round(sum(vals),5),"profit_factor":round(sum(wins)/-sum(losses),4) if losses else None,"best":sorted([{"symbol":t.get("symbol"),"net_pct":t["net_pct"],"reason":t.get("exit_reason")} for t in closed],key=lambda x:x["net_pct"],reverse=True)[:5],"worst":sorted([{"symbol":t.get("symbol"),"net_pct":t["net_pct"],"reason":t.get("exit_reason")} for t in closed],key=lambda x:x["net_pct"])[:5]}),flush=True)
