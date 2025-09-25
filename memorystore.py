#!/usr/bin/env python3
"""
memorystore.py

Collects Memorystore metrics for Redis, Valkey, and Redis Cluster using ONLY Cloud Monitoring.
No direct connections are made to instances. Requires only roles/monitoring.viewer.

- Reuses command categorization from the attached msstats.py (imported).
- Computes the same high-level command categories that msstats.py outputs via:
  processMetricPoint() + processNodeStats().

For each node (instance node or cluster node), the script outputs a CSV row containing:
  Source, Project ID, InstanceType, ClusterId, InstanceId, NodeId, NodeRole, Region, Zone, NodeType,
  BytesUsedForCache, MaxMemory, and the command-category columns from msstats.py.

Usage:
  python memorystore.py --project YOUR_PROJECT_ID --credentials /path/to/sa.json --out /path/to/out.csv
Optional:
  --duration 604800   # lookback window in seconds (default 7 days)
  --step 60           # alignment step in seconds for rate metrics (default 60)

"""
import argparse
import csv
import os
import sys
import time
from typing import Dict, List, Optional, Any
from collections import defaultdict

from google.oauth2 import service_account
from google.cloud import monitoring_v3

# Import the categorization logic from the provided msstats.py
# We rely on these to compute the exact same metrics/categories.
try:
    import msstats as ms
except ImportError as ex:
    print(
        f"Error: msstats.py not found in PYTHONPATH. Place msstats.py next to this script. {ex}"
    )
    sys.exit(1)

# ---------- Metric maps per product ----------
# Redis (non-cluster)
REDIS_METRICS = {
    "commands": "redis.googleapis.com/commands/calls",
    "memory_usage": "redis.googleapis.com/stats/memory/usage",
    "max_memory": "redis.googleapis.com/stats/memory/maxmemory",
}
# Valkey (Memorystore for Valkey) - use node-level for commands & usage; instance-level for size.
VALKEY_METRICS = {
    "commands": "memorystore.googleapis.com/instance/node/commandstats/calls_count",
    "memory_usage": "memorystore.googleapis.com/instance/node/memory/usage",
    "max_memory": "memorystore.googleapis.com/instance/memory/size",
}
# Redis Cluster - use node-level for commands & usage; cluster-level for size.
CLUSTER_METRICS = {
    "commands": "redis.googleapis.com/cluster/node/commandstats/calls_count",
    "memory_usage": "redis.googleapis.com/cluster/node/memory/usage",
    "max_memory": "redis.googleapis.com/cluster/memory/size",
}

# Helper label candidates
REGION_LABELS = ("region", "location")
ZONE_LABELS = ("zone",)
NODETYPE_LABELS = (
    "node_type",
    "cluster_node_type",
    "tier",
    "service_tier",
    "instance_type",
)


def _pick(labels: Dict[str, str], keys) -> Optional[str]:
    for k in keys:
        v = labels.get(k)
        if v:
            return v
    return None


def _time_interval(duration_sec: int) -> monitoring_v3.TimeInterval:
    now = time.time()
    seconds = int(now)
    nanos = int((now - seconds) * 10**9)
    return monitoring_v3.TimeInterval(
        {
            "end_time": {"seconds": seconds, "nanos": nanos},
            "start_time": {"seconds": (seconds - duration_sec), "nanos": nanos},
        }
    )


def _make_rate_aggregation(step_sec: int) -> monitoring_v3.Aggregation:
    return monitoring_v3.Aggregation(
        {
            "alignment_period": {"seconds": step_sec},
            "per_series_aligner": monitoring_v3.Aggregation.Aligner.ALIGN_RATE,
            "cross_series_reducer": monitoring_v3.Aggregation.Reducer.REDUCE_NONE,
        }
    )


def _list_ts(
    client: monitoring_v3.MetricServiceClient,
    project_name: str,
    metric_type: str,
    interval: monitoring_v3.TimeInterval,
    view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
    aggregation: Optional[monitoring_v3.Aggregation] = None,
):
    req = {
        "name": project_name,
        "filter": f'metric.type = "{metric_type}"',
        "interval": interval,
        "view": view,
    }
    if aggregation is not None:
        req["aggregation"] = aggregation
    return list(client.list_time_series(request=req))


def _ensure_node_entry(
    table: Dict[str, Dict[str, Dict[str, Any]]], inst_key: str, node_id: str
) -> Dict[str, Any]:
    if inst_key not in table:
        table[inst_key] = {}
    if node_id not in table[inst_key]:
        table[inst_key][node_id] = {
            "Source": "MS",
            "ClusterId": inst_key,  # for Redis this is the instance name; for Cluster this is the cluster name
            "NodeId": node_id,
            "NodeRole": "",
            "NodeType": "",
            "Region": "",
            "Zone": "",
            "Project ID": "",  # filled later
            "InstanceId": "",  # full resource name if available
            "InstanceType": "",  # Redis | Valkey | Redis Cluster
            "points": {},  # timestamp -> {cmd: rate}
        }
    return table[inst_key][node_id]


def _accumulate_commands(results, table, product_name: str, project_id: str):
    """
    Accumulate per-node command rates into table[instance][node]["points"][timestamp][cmd] = rate
    """
    for ts in results:
        rlabels = dict(ts.resource.labels)
        mlabels = dict(ts.metric.labels)

        # Identify instance/cluster id & node
        inst_key = (
            rlabels.get("instance_id")
            or rlabels.get("cluster_id")
            or rlabels.get("resource_name")
            or "unknown"
        )
        node_id = rlabels.get("node_id") or rlabels.get("shard_id") or "unknown"
        entry = _ensure_node_entry(table, inst_key, node_id)

        # Fill common attributes
        entry["Project ID"] = project_id
        entry["InstanceId"] = (
            rlabels.get("instance_id") or rlabels.get("cluster_id") or ""
        )
        entry["Region"] = _pick(rlabels, REGION_LABELS) or entry["Region"]
        entry["Zone"] = _pick(rlabels, ZONE_LABELS) or entry["Zone"]
        entry["NodeType"] = _pick(rlabels, NODETYPE_LABELS) or entry["NodeType"]

        # Node role if provided (e.g., 'primary'/'replica')
        role = mlabels.get("role") or rlabels.get("role") or ""
        if role:
            entry["NodeRole"] = (
                "Master"
                if role == "primary"
                else ("Replica" if role == "replica" else role)
            )

        # Instance type label
        entry["InstanceType"] = product_name

        # Command name label is typically 'cmd'
        cmd = mlabels.get("cmd")
        if not cmd:
            # best effort alternative label naming
            for alt in ("command", "command_name"):
                if alt in mlabels:
                    cmd = mlabels[alt]
                    break
        if not cmd:
            continue

        # Collect points as rates
        for point in ts.points:
            t = point.interval.start_time.timestamp()
            if t not in entry["points"]:
                entry["points"][t] = {}
            # Support both int/double values
            pv = 0.0
            try:
                pv = point.value.double_value
            except Exception:
                try:
                    pv = float(point.value.int64_value)
                except Exception:
                    pv = 0.0
            entry["points"][t][cmd] = pv


def _apply_processed_categories(table):
    """
    For each node entry that has points, compute processed per-timestamp categories using
    ms.processMetricPoint(), then reduce with ms.processNodeStats() (max across window).
    Store the dict under entry["commandstats"] and remove 'points'.
    """
    for inst in list(table.keys()):
        for node in list(table[inst].keys()):
            entry = table[inst][node]
            processed = {}
            for ts, cmdmap in entry.get("points", {}).items():
                processed[ts] = ms.processMetricPoint(cmdmap)
            entry["commandstats"] = ms.processNodeStats(processed) if processed else {}
            if "points" in entry:
                del entry["points"]


def _attach_memory_usage(results, table, key_name="BytesUsedForCache"):
    for ts in results:
        rlabels = dict(ts.resource.labels)
        inst_key = (
            rlabels.get("instance_id")
            or rlabels.get("cluster_id")
            or rlabels.get("resource_name")
            or "unknown"
        )
        node_id = rlabels.get("node_id") or rlabels.get("shard_id") or "unknown"
        if inst_key not in table or node_id not in table[inst_key]:
            _ensure_node_entry(table, inst_key, node_id)
        entry = table[inst_key][node_id]
        # take the max usage observed
        maxv = 0
        for point in ts.points:
            try:
                v = int(point.value.int64_value)
            except Exception:
                try:
                    v = int(point.value.double_value)
                except Exception:
                    v = 0
            if v > maxv:
                maxv = v
        prev = entry.get(key_name, 0)
        entry[key_name] = max(prev, maxv)


def _attach_capacity_scalar(results, table, key_name="MaxMemory"):
    """Attach a capacity scalar (e.g., memory size); applies to all nodes within the instance/cluster."""
    cap_by_inst = defaultdict(int)
    for ts in results:
        rlabels = dict(ts.resource.labels)
        inst_key = (
            rlabels.get("instance_id")
            or rlabels.get("cluster_id")
            or rlabels.get("resource_name")
            or "unknown"
        )
        v_max = 0
        for point in ts.points:
            try:
                v = int(point.value.int64_value)
            except Exception:
                try:
                    v = int(point.value.double_value)
                except Exception:
                    v = 0
            if v > v_max:
                v_max = v
        if v_max > cap_by_inst[inst_key]:
            cap_by_inst[inst_key] = v_max

    for inst_key, nodes in table.items():
        if inst_key in cap_by_inst:
            for node_id in nodes:
                nodes[node_id][key_name] = cap_by_inst[inst_key]


def _flatten_rows(table, project_id: str, instance_type: str) -> List[Dict[str, Any]]:
    rows = []
    for inst_key, nodes in table.items():
        for node_id, entry in nodes.items():
            entry["Project ID"] = project_id or entry.get("Project ID", "")
            entry["InstanceType"] = instance_type or entry.get("InstanceType", "")
            row = {**entry}
            row.update(entry.get("commandstats", {}))
            row.pop("commandstats", None)
            rows.append(row)
    return rows


def collect_for_product(
    client,
    project_id: str,
    duration: int,
    step: int,
    metric_map: Dict[str, str],
    instance_type_label: str,
) -> List[Dict[str, Any]]:
    project_name = f"projects/{project_id}"
    interval = _time_interval(duration)
    agg = _make_rate_aggregation(step)

    table: Dict[str, Dict[str, Dict[str, Any]]] = {}

    # Commands (primary discovery)
    try:
        cmd_results = _list_ts(
            client,
            project_name,
            metric_map["commands"],
            interval,
            view=monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
            aggregation=agg,
        )
    except Exception:
        cmd_results = []
    _accumulate_commands(cmd_results, table, instance_type_label, project_id)

    # If nothing found, discover via memory usage series (so we still emit rows)
    if not table:
        try:
            mem_results = _list_ts(
                client, project_name, metric_map["memory_usage"], interval
            )
        except Exception:
            mem_results = []
        _attach_memory_usage(mem_results, table)
        for inst_key, nodes in table.items():
            for node_id, entry in nodes.items():
                entry["InstanceType"] = instance_type_label
                entry["Project ID"] = project_id

    # Memory usage (BytesUsedForCache)
    try:
        mem_results = _list_ts(
            client, project_name, metric_map["memory_usage"], interval
        )
        _attach_memory_usage(mem_results, table)
    except Exception:
        pass

    # Capacity (MaxMemory) - instance/cluster level
    try:
        cap_results = _list_ts(client, project_name, metric_map["max_memory"], interval)
        _attach_capacity_scalar(cap_results, table, key_name="MaxMemory")
    except Exception:
        pass

    # Compute command categories
    _apply_processed_categories(table)

    # Flatten to rows
    return _flatten_rows(table, project_id, instance_type_label)


def main():
    parser = argparse.ArgumentParser(
        description="Export Memorystore metrics for Redis, Valkey and Redis Cluster to CSV (using only Cloud Monitoring)."
    )
    parser.add_argument("--project", required=True, help="GCP Project ID")
    parser.add_argument(
        "--credentials",
        required=True,
        help="Path to service account JSON with monitoring.viewer",
    )
    parser.add_argument("--out", required=True, help="Output CSV file path")
    parser.add_argument(
        "--duration",
        type=int,
        default=604800,
        help="Lookback window in seconds (default 7 days)",
    )
    parser.add_argument(
        "--step",
        type=int,
        default=60,
        help="Alignment step in seconds for rate metrics (default 60)",
    )
    args = parser.parse_args()

    # Auth
    creds = service_account.Credentials.from_service_account_file(args.credentials)
    client = monitoring_v3.MetricServiceClient(credentials=creds)

    all_rows: List[Dict[str, Any]] = []

    # Collect for each product
    for metric_map, label in (
        (REDIS_METRICS, "Redis"),
        (VALKEY_METRICS, "Valkey"),
        (CLUSTER_METRICS, "Redis Cluster"),
    ):
        rows = collect_for_product(
            client, args.project, args.duration, args.step, metric_map, label
        )
        all_rows.extend(rows)

    if not all_rows:
        print("Warning: No metrics found; CSV will be created with no rows.")

    # Build header: union of keys across rows, with useful columns first
    base_order = [
        "Source",
        "Project ID",
        "InstanceType",
        "ClusterId",
        "InstanceId",
        "NodeId",
        "NodeRole",
        "NodeType",
        "Region",
        "Zone",
        "BytesUsedForCache",
        "MaxMemory",
    ]
    category_keys = []
    for row in all_rows:
        for k in row.keys():
            if k not in base_order and k not in category_keys:
                category_keys.append(k)
    header = base_order + category_keys

    # Write CSV
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    with open(args.out, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for row in all_rows:
            writer.writerow(row)

    print(f"Wrote {len(all_rows)} rows to {args.out}")


if __name__ == "__main__":
    sys.exit(main())
