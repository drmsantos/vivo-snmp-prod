#!/usr/bin/env python3
# =============================================================================
# Autor:   Diego Regis M. F. dos Santos
# Email:   diego-f-santos@openlabs.com.br
# Time:    OpenLabs - DevOps | Infra | Versão: 5.0
# Desc:    Autodiscovery SNMP via CSV — sem K8s API, sem RBAC
#          Coleta via IP eth0 dos pods (Flannel) — sem problemas de macvlan
#          Em produção: trocar IPs do CSV pelos IPs OAM reais dos switches
#          IMPORTANTE: required_acks=1 obrigatório para Kafka single-broker
# =============================================================================
import os, csv, sys

SNMP_PORT     = os.getenv("SNMP_PORT", "161")
COMMUNITY     = os.getenv("COMMUNITY", "public")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka.snmp-monitor.svc.cluster.local:9092")
OUTPUT        = os.getenv("TELEGRAF_CONF", "/etc/telegraf/telegraf.conf")
CSV_PATH      = os.getenv("SWITCHES_CSV", "/etc/snmp/switches.csv")

EXTRA_FIELDS = {
    "transport": ["optRxPower", "optTxPower", "chassisTemp"],
    "internet":  ["bgpPeers"],
}

def load_csv():
    agents = {}
    with open(CSV_PATH) as f:
        for row in csv.DictReader(f):
            ip    = row.get("ip","").strip()
            layer = row.get("layer","").strip().lower()
            port  = row.get("port", SNMP_PORT).strip()
            comm  = row.get("community", COMMUNITY).strip()
            if not ip or not layer: continue
            agents.setdefault(layer, {"ips": [], "community": comm})
            agents[layer]["ips"].append(f'"udp://{ip}:{port}"')
            print(f"  [{layer}] {ip}:{port}")
    return agents

def snmp_block(layer, info):
    ips  = info["ips"]
    comm = info.get("community", COMMUNITY)
    if not ips: return ""
    extra = EXTRA_FIELDS.get(layer, [])

    block = f"""
[[inputs.snmp]]
  name_override  = "snmp_{layer}"
  agents         = [{",".join(ips)}]
  version        = 2
  community      = "{comm}"
  timeout        = "5s"
  retries        = 0
  agent_host_tag = "source"

  [[inputs.snmp.field]]
    name = "sysDescr"
    oid  = "RFC1213-MIB::sysDescr.0"
  [[inputs.snmp.field]]
    name = "sysName"
    oid  = "RFC1213-MIB::sysName.0"
  [[inputs.snmp.field]]
    name = "sysUpTime"
    oid  = "RFC1213-MIB::sysUpTime.0"
  [[inputs.snmp.field]]
    name = "cpuUsage"
    oid  = "NET-SNMP-EXTEND-MIB::nsExtendOutput1Line.\\"cpuUsage\\""
  [[inputs.snmp.field]]
    name = "memUsage"
    oid  = "NET-SNMP-EXTEND-MIB::nsExtendOutput1Line.\\"memUsage\\""
"""
    for f in extra:
        block += f"""  [[inputs.snmp.field]]
    name = "{f}"
    oid  = "NET-SNMP-EXTEND-MIB::nsExtendOutput1Line.\\"{f}\\""\n"""

    return block

def main():
    print("=== Autodiscovery SNMP via CSV ===")
    agents = load_csv()
    if not agents:
        print("ERRO: CSV vazio ou não encontrado")
        sys.exit(1)

    conf = f"""[agent]
  interval            = "60s"
  round_interval      = false
  metric_batch_size   = 1000
  metric_buffer_limit = 20000
  flush_interval      = "30s"
  hostname            = "telegraf-vivo"
  omit_hostname       = false

[[outputs.kafka]]
  brokers       = ["{KAFKA_BROKERS}"]
  topic         = "snmp-metrics"
  data_format   = "json"
  required_acks = 1

"""
    total = 0
    for layer, info in agents.items():
        conf += snmp_block(layer, info)
        total += len(info["ips"])
        print(f"  {layer}: {len(info['ips'])} switches")

    with open(OUTPUT, "w") as f:
        f.write(conf)
    print(f"telegraf.conf gerado — total: {total} switches")

if __name__ == "__main__":
    main()
