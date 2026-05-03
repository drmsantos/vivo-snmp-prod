#!/bin/bash
# =============================================================================
# Autor:   Diego Regis M. F. dos Santos
# Email:   diego-f-santos@openlabs.com.br
# Time:    OpenLabs - DevOps | Infra | Versão: 2.2
# Uso:     ./deploy.sh [--apply | --destroy | --status | --update-csv]
#
# IMPORTANTE — Ordem de operações após --apply:
#   1. Aguardar TimescaleDB inicializar (~30s)
#   2. Rodar --update-csv para popular o CSV com IPs reais dos pods
#   3. Os dados começarão a fluir em ~60s
# =============================================================================
set -euo pipefail

NS_TARGETS="snmp-targets"
NS_MONITOR="snmp-monitor"

case "${1:-}" in

  --apply)
    echo "=== [1/2] snmp-targets (switches simulados) ==="
    kubectl apply -f targets/manifests/01-namespace-networks.yaml
    kubectl apply -f targets/manifests/02-switches.yaml
    echo "Aguardando switches subirem (30s)..."
    sleep 30
    kubectl get pods -n $NS_TARGETS --no-headers | grep -v Running | grep -v Completed || echo "Todos os switches Running!"

    echo ""
    echo "=== [2/2] snmp-monitor (infraestrutura) ==="
    kubectl apply -f monitor/manifests/01-namespace.yaml
    kubectl apply -f monitor/manifests/02-timescaledb.yaml
    echo "Aguardando TimescaleDB (90s)..."
    kubectl wait --for=condition=available deployment/timescaledb \
      -n $NS_MONITOR --timeout=180s

    kubectl apply -f monitor/manifests/03-kafka.yaml
    echo "Aguardando Kafka (60s)..."
    kubectl wait --for=condition=available deployment/kafka \
      -n $NS_MONITOR --timeout=120s

    kubectl apply -f monitor/manifests/04-telegraf.yaml
    kubectl apply -f monitor/manifests/05-kafka-consumer.yaml
    kubectl apply -f monitor/manifests/06-noc.yaml

    echo ""
    echo "=== Deploy completo! ==="
    echo ""
    echo "PRÓXIMO PASSO OBRIGATÓRIO:"
    echo "  ./deploy.sh --update-csv"
    echo ""
    echo "Dashboard: http://vivo-noc.172.16.15.32.nip.io"
    ;;

  --update-csv)
    echo "=== Coletando IPs reais dos switches ==="
    echo "ip,layer,community,port" > /tmp/switches-all.csv
    for layer in sw-core sw-pe sw-access sw-transport sw-internet sw-dc; do
      l=$(echo $layer | sed 's/sw-//')
      for pod in $(kubectl get pods -n $NS_TARGETS -l app=$layer -o name 2>/dev/null); do
        # Usa IP eth0 (Flannel) — funciona cross-node sem macvlan hair-pinning
        ip=$(kubectl get $pod -n $NS_TARGETS -o jsonpath='{.status.podIP}' 2>/dev/null)
        [ -n "$ip" ] && echo "$ip,$l,public,161" >> /tmp/switches-all.csv
      done
    done

    # Deduplica
    head -1 /tmp/switches-all.csv > /tmp/switches-dedup.csv
    tail -n +2 /tmp/switches-all.csv | sort -u >> /tmp/switches-dedup.csv

    echo "CSV gerado ($(tail -n +2 /tmp/switches-dedup.csv | wc -l) switches):"
    cat /tmp/switches-dedup.csv

    kubectl create configmap switches-csv \
      --from-file=switches.csv=/tmp/switches-dedup.csv \
      -n $NS_MONITOR --dry-run=client -o yaml | kubectl apply -f -

    kubectl rollout restart daemonset/telegraf -n $NS_MONITOR
    echo "CSV atualizado e Telegraf reiniciado"
    echo "Aguardando 90s para primeiros dados..."
    sleep 90
    kubectl exec -n $NS_MONITOR deploy/timescaledb -- \
      psql -U telegraf -d snmp_metrics -c "\dt" 2>/dev/null || echo "TimescaleDB inicializando..."
    ;;

  --destroy)
    echo "=== Destruindo ambiente ==="
    kubectl delete namespace $NS_TARGETS --ignore-not-found
    kubectl delete namespace $NS_MONITOR --ignore-not-found
    echo "Ambiente destruído"
    ;;

  --status)
    echo "=== snmp-targets ==="
    kubectl get pods -n $NS_TARGETS -o wide 2>/dev/null | head -40
    echo ""
    echo "=== snmp-monitor ==="
    kubectl get pods -n $NS_MONITOR -o wide 2>/dev/null
    echo ""
    echo "=== Pipeline ==="
    kubectl exec -n $NS_MONITOR deploy/timescaledb -- \
      psql -U telegraf -d snmp_metrics -c "
        SELECT
          (SELECT COUNT(*) FROM snmp_core)      AS core,
          (SELECT COUNT(*) FROM snmp_pe)        AS pe,
          (SELECT COUNT(*) FROM snmp_access)    AS access,
          (SELECT COUNT(*) FROM snmp_transport) AS transport,
          (SELECT COUNT(*) FROM snmp_internet)  AS internet,
          (SELECT COUNT(*) FROM snmp_dc)        AS dc
        " 2>/dev/null || echo "TimescaleDB inicializando..."
    ;;

  *)
    echo "Uso: $0 [--apply | --destroy | --status | --update-csv]"
    ;;
esac
