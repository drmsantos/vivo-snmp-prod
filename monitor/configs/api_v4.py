# =============================================================================
# Autor:   Diego Regis M. F. dos Santos
# Email:   diego-f-santos@openlabs.com.br
# Time:    OpenLabs - DevOps | Infra
# Versão:  3.0
# Desc:    API NOC 100% dinâmica — autodiscovery completo
#          Nenhuma rede hardcoded. Adicione rede no Telegraf = aparece no NOC
# =============================================================================
import os, psycopg2, psycopg2.extras, re, time
from flask import Flask, jsonify
from flask_cors import CORS
from datetime import datetime, timezone

app = Flask(__name__)
CORS(app)

DB_DSN = os.getenv("DB_DSN",
    "host=postgresql.snmp-lab.svc.cluster.local "
    "dbname=snmp_lab user=telegraf password=telegraf123")

CACHE_TTL = 300
_cache = {"data": None, "ts": 0}

VENDOR_RULES = [
    (r"nokia|7750|sr os",               "Nokia",    "#3d8eff"),
    (r"huawei|vrp|ne40|ne80",           "Huawei",   "#00d4aa"),
    (r"cisco ios xr|asr.9",             "Cisco ASR","#fb923c"),
    (r"cisco ios xe|catalyst|c9[23]00", "Cisco",    "#a78bfa"),
    (r"arista|eos",                     "Arista",   "#34d399"),
    (r"ciena|6500|waveserver",          "Ciena",    "#f59e0b"),
    (r"juniper|junos|mx[0-9]",          "Juniper",  "#e879f9"),
    (r"ericsson|mini-link",             "Ericsson", "#38bdf8"),
]
COLORS = ["#3d8eff","#00d4aa","#a78bfa","#f59e0b","#fb923c","#34d399","#e879f9","#38bdf8","#f472b6","#facc15"]

def infer_vendor(descr):
    if not descr: return "Unknown", "#64748b"
    d = descr.lower()
    for pat, v, c in VENDOR_RULES:
        if re.search(pat, d): return v, c
    return "Unknown", "#64748b"

def layer_name(table, prefix):
    t = table.lower().replace("snmp_", "")
    names = {"core":"Core","pe":"PE","access":"Access","transport":"Transporte",
             "internet":"Internet","dc":"Data Center","mgmt":"OAM","oam":"OAM"}
    if t in names: return names[t]
    parts = prefix.split(".")
    return f"Rede {parts[2]}" if len(parts) >= 3 else t.capitalize()

def get_db():
    return psycopg2.connect(DB_DSN)

def discover():
    now = time.time()
    if _cache["data"] and now - _cache["ts"] < CACHE_TTL:
        return _cache["data"]
    try:
        conn = get_db()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'snmp_%' ORDER BY tablename")
        tables = [r["tablename"] for r in cur.fetchall()]
        layers = {}
        ci = 0
        for table in tables:
            try:
                col = "sysDescr"
                q = ("SELECT " + chr(34) + col + chr(34) +
                     " FROM " + table +
                     " WHERE time >= NOW() AT TIME ZONE 'UTC' - INTERVAL '15 minutes'"
                     " AND source IS NOT NULL LIMIT 1")
                cur.execute(q)
                r = cur.fetchone()
                if not r: continue
                vendor, color = infer_vendor(r[col])
                if vendor == "Unknown":
                    color = COLORS[ci % len(COLORS)]; ci += 1
                layer_id = table.replace("snmp_", "")
                lnames = {"core":"Core","pe":"PE","access":"Access",
                          "transport":"Transporte","internet":"Internet","dc":"Data Center"}
                lname = lnames.get(layer_id, layer_id.capitalize())
                layers[table] = {"id": layer_id, "name": lname,
                    "vendor": vendor, "color": color, "table": table, "prefix": table}
            except:
                try: conn.rollback()
                except: pass
                continue
        cur.close(); conn.close()
        _cache["data"] = layers; _cache["ts"] = now
        return layers
    except: return {}

def calc_throughput(cur, table, prefix=None, ip_filter=None):
    try:
        cur.execute(
            "SELECT column_name FROM information_schema.columns"
            " WHERE table_name=%s AND column_name='ifInOctets'",
            (table,)
        )
        if not cur.fetchone():
            return {}
        cur.execute(
            "SELECT DISTINCT ON (source) source,"
            " CAST(\"ifInOctets\" AS FLOAT) AS i,"
            " CAST(\"ifOutOctets\" AS FLOAT) AS o, time"
            " FROM " + table +
            " WHERE source IS NOT NULL"
            " AND time >= NOW() AT TIME ZONE 'UTC' - INTERVAL '10 minutes'"
            " AND \"sysName\" IS NOT NULL"
            " ORDER BY source, time DESC"
        )
        latest = {r["source"]: r for r in cur.fetchall()}
        cur.execute(
            "SELECT DISTINCT ON (source) source,"
            " CAST(\"ifInOctets\" AS FLOAT) AS i,"
            " CAST(\"ifOutOctets\" AS FLOAT) AS o, time"
            " FROM " + table +
            " WHERE source IS NOT NULL"
            " AND time >= NOW() AT TIME ZONE 'UTC' - INTERVAL '15 minutes'"
            " AND time <  NOW() AT TIME ZONE 'UTC' - INTERVAL '3 minutes'"
            " ORDER BY source, time DESC"
        )
        prev = {r["source"]: r for r in cur.fetchall()}
        result = {}
        for ip, r in latest.items():
            in_m = out_m = 0
            if ip in prev and prev[ip]["i"] and r["i"]:
                dt = (r["time"] - prev[ip]["time"]).total_seconds()
                if dt > 0:
                    in_m  = round(max(0, (r["i"] - prev[ip]["i"]) * 8 / 1e6 / dt), 4)
                    out_m = round(max(0, (r["o"] - prev[ip]["o"]) * 8 / 1e6 / dt), 4)
            result[ip] = {"in_mbps": in_m, "out_mbps": out_m}
        return result
    except:
        return {}

def build_links(nodes):
    # Nível hierárquico pelo terceiro octeto
    layer_level = {}
    for n in nodes:
        p = n["ip"].split(".")
        if len(p) >= 3:
            oct3 = int(p[2])
            lvl  = 0 if oct3 <= 2 else 1 if oct3 <= 5 else 2
            layer_level[n["layer"]] = min(layer_level.get(n["layer"], 99), lvl)

    layer_nodes = {}
    for n in nodes: layer_nodes.setdefault(n["layer"], []).append(n)

    sorted_layers = sorted(layer_level.items(), key=lambda x: x[1])
    links = []; seen = set()
    for i in range(len(sorted_layers) - 1):
        sl, dl = sorted_layers[i][0], sorted_layers[i+1][0]
        srcs = layer_nodes.get(sl, [])
        dsts = layer_nodes.get(dl, [])
        for j, s in enumerate(srcs):
            for k, d in enumerate(dsts):
                key = f"{s['ip']}-{d['ip']}"
                if key not in seen and k % max(1,len(dsts)//max(1,len(srcs))) == j % max(1,len(srcs)):
                    seen.add(key)
                    tp = round((s.get("out_mbps",0) + d.get("in_mbps",0)) / 2, 2)
                    links.append({"source": s["ip"], "target": d["ip"], "throughput": tp})
    return links

@app.route("/api/topology")
def api_topology():
    try:
        layers = discover()
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        nodes = []
        for prefix, info in layers.items():
            try:
                cur.execute(f"""
                    SELECT DISTINCT ON (source) source,
                        "sysName","sysDescr","sysUpTime",
                        CAST("cpuUsage" AS FLOAT) AS cpu,
                        CAST("memUsage" AS FLOAT) AS mem, time
                    FROM {info['table']}
                    WHERE source IS NOT NULL
                      AND time >= NOW() AT TIME ZONE 'UTC' - INTERVAL '10 minutes'
                      AND "sysName" IS NOT NULL
                    ORDER BY source, time DESC
                """, (f"{prefix}.%",))
                latest = cur.fetchall()
                tp = calc_throughput(cur, info["table"], prefix)
                for r in latest:
                    ip  = r["source"]
                    cpu = round(float(r["cpu"] or 0), 1)
                    mem = round(float(r["mem"] or 0), 1)
                    vendor, color = infer_vendor(r.get("sysDescr",""))
                    if vendor == "Unknown": vendor, color = info["vendor"], info["color"]
                    t = tp.get(ip, {})
                    nodes.append({
                        "id": ip, "name": r["sysName"], "ip": ip,
                        "layer": info["id"], "layer_name": info["name"],
                        "vendor": vendor, "color": color,
                        "cpu": cpu, "mem": mem,
                        "in_mbps":  t.get("in_mbps", 0),
                        "out_mbps": t.get("out_mbps", 0),
                        "uptime": int(r["sysUpTime"] or 0),
                        "status": "critical" if cpu>80 else "warning" if cpu>60 else "ok",
                        "last_seen": r["time"].isoformat() if r["time"] else None,
                    })
            except:
                try: conn.rollback()
                except: pass
                continue
        cur.close(); conn.close()
        return jsonify({
            "ok": True, "nodes": nodes, "links": build_links(nodes),
            "layers": list(layers.values()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/history/<path:ip>")
def api_history(ip):
    try:
        layers = discover()
        prefix = ".".join(ip.split(".")[:3])
        info   = layers.get(prefix)
        if not info: return jsonify({"ok": False, "error": "not found"}), 404
        conn = get_db()
        cur  = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute(f"""
            SELECT time, CAST("cpuUsage" AS FLOAT) AS cpu, CAST("memUsage" AS FLOAT) AS mem,
                CAST("ifInOctets" AS FLOAT) AS i, CAST("ifOutOctets" AS FLOAT) AS o
            FROM {info['table']}
            WHERE source=%s AND time >= NOW() AT TIME ZONE 'UTC' - INTERVAL '30 minutes'
              AND "cpuUsage" IS NOT NULL
            ORDER BY time ASC
        """, (ip,))
        rows = cur.fetchall(); cur.close(); conn.close()
        history = []
        for idx, r in enumerate(rows):
            in_m = out_m = 0
            if idx > 0:
                p  = rows[idx-1]
                dt = (r["time"] - p["time"]).total_seconds()
                if dt > 0 and p["i"] and r["i"]:
                    in_m  = round(max(0, (r["i"]-p["i"])*8/1e6/dt), 2)
                    out_m = round(max(0, (r["o"]-p["o"])*8/1e6/dt), 2)
            history.append({"time": r["time"].strftime("%H:%M:%S"),
                "cpu": round(float(r["cpu"] or 0),1), "mem": round(float(r["mem"] or 0),1),
                "in_mbps": in_m, "out_mbps": out_m})
        return jsonify({"ok": True, "data": history[-60:]})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/alerts")
def api_alerts():
    try:
        layers = discover()
        conn = get_db(); cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        alerts = []
        for prefix, info in layers.items():
            try:
                cur.execute(f"""
                    SELECT DISTINCT ON (source) source, "sysName",
                        CAST("cpuUsage" AS FLOAT) AS cpu, CAST("memUsage" AS FLOAT) AS mem, time
                    FROM {info['table']}
                    WHERE source IS NOT NULL AND time >= NOW() AT TIME ZONE 'UTC' - INTERVAL '10 minutes'
                      AND (CAST("cpuUsage" AS FLOAT)>80 OR CAST("memUsage" AS FLOAT)>85)
                    ORDER BY source, time DESC
                """, (f"{prefix}.%",))
                for r in cur.fetchall():
                    cpu = round(float(r["cpu"] or 0),1); mem = round(float(r["mem"] or 0),1)
                    agent = r["sysName"] or r["source"]
                    if cpu > 80: alerts.append({"severity":"critical","layer":info["name"],"color":info["color"],"agent":agent,"message":f"CPU {cpu}%","time":r["time"].isoformat()})
                    if mem > 85: alerts.append({"severity":"warning","layer":info["name"],"color":info["color"],"agent":agent,"message":f"Mem {mem}%","time":r["time"].isoformat()})
            except:
                try: conn.rollback()
                except: pass
                continue
        cur.close(); conn.close()
        return jsonify({"ok": True, "data": alerts, "count": len(alerts)})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

@app.route("/api/layers")
def api_layers():
    layers = discover()
    return jsonify({"ok": True, "count": len(layers), "layers": list(layers.values())})

@app.route("/api/health")
def health():
    return jsonify({"ok": True, "version": "3.0", "mode": "autodiscovery"})


@app.route("/dashboard")
def dashboard():
    import os
    for path in ["/tmp/shared/dashboard.html","/tmp/dashboard.html"]:
        if os.path.exists(path):
            with open(path,"rb") as f: return f.read(),200,{"Content-Type":"text/html; charset=utf-8"}
    return "Dashboard nao encontrado",404
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)

