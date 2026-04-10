import csv
import os


def _round_number(value):
    """숫자 값은 소수점 4자리까지 반올림한다."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return round(value, 4)
    return value


def _safe_get(mapping, *keys, default=None):
    """중첩 dict 조회 시 KeyError 없이 기본값을 반환한다."""
    current = mapping
    for key in keys:
        if not isinstance(current, dict):
            return default
        current = current.get(key)
        if current is None:
            return default
    return current


def _write_csv(path, header, rows):
    """엑셀 호환을 위해 utf-8-sig로 CSV를 저장한다."""
    with open(path, "w", newline="", encoding="utf-8-sig") as file_obj:
        writer = csv.writer(file_obj)
        writer.writerow(header)
        for row in rows:
            writer.writerow([_round_number(value) for value in row])


def export_results(collector, config, output_dir):
    """시뮬레이션 결과를 CSV 9종으로 저장한다."""
    # 출력 폴더가 없으면 생성한다.
    os.makedirs(output_dir, exist_ok=True)

    summary = collector.get_summary() if hasattr(collector, "get_summary") else {}
    data_summary = config.get("data_summary", {}) if isinstance(config, dict) else {}
    real_process_stats = data_summary.get("real_process_stats", {}) if isinstance(data_summary, dict) else {}
    common_routing = config.get("common_routing", []) if isinstance(config, dict) else []
    common_routing_text = " -> ".join(common_routing) if common_routing else ""

    utilization = collector.get_utilization(config) if hasattr(collector, "get_utilization") else {}
    due_perf = collector.get_due_date_performance(config) if hasattr(collector, "get_due_date_performance") else {}
    avg_util = round(
        sum(u["utilization_rate"] for u in utilization.values()) / len(utilization), 4
    ) if utilization else 0.0

    kpi_header = [
        "throughput",
        "defect_count",
        "defect_rate",
        "avg_cycle_time",
        "median_cycle_time",
        "max_cycle_time",
        "avg_wip",
        "total_cost",
        "sim_duration_min",
        "real_case_count",
        "common_routing",
        "avg_utilization_rate",
        "due_date_adherence_rate",
    ]
    kpi_rows = [[
        summary.get("throughput", 0),
        summary.get("defect_count", 0),
        summary.get("defect_rate", 0),
        summary.get("avg_cycle_time", 0),
        summary.get("median_cycle_time", 0),
        summary.get("max_cycle_time", 0),
        summary.get("avg_wip", 0),
        summary.get("total_cost", 0),
        _safe_get(config, "sim", "duration", default=0),
        data_summary.get("case_count", 0) if isinstance(data_summary, dict) else 0,
        common_routing_text,
        avg_util,
        due_perf.get("due_date_adherence_rate", 0),
    ]]
    _write_csv(os.path.join(output_dir, "kpi_summary.csv"), kpi_header, kpi_rows)

    stage_stats = summary.get("stage_stats", {}) if isinstance(summary, dict) else {}
    process_names = sorted(set(list(stage_stats.keys()) + list(real_process_stats.keys())))
    process_header = [
        "process_name",
        "completed",
        "avg_wait_time_min",
        "avg_process_time_min",
        "defect_rate",
        "avg_queue_length",
        "real_mean_min",
        "real_defect_rate",
        "real_throughput_rows",
    ]
    process_rows = []
    for process_name in process_names:
        sim_row = stage_stats.get(process_name, {}) if isinstance(stage_stats, dict) else {}
        real_row = real_process_stats.get(process_name, {}) if isinstance(real_process_stats, dict) else {}
        process_rows.append([
            process_name,
            sim_row.get("completed", 0),
            sim_row.get("avg_wait_time", 0),
            sim_row.get("avg_process_time", 0),
            sim_row.get("defect_rate", 0),
            sim_row.get("avg_queue_length", 0),
            real_row.get("mean_minutes", 0),
            real_row.get("defect_rate", 0),
            real_row.get("throughput_rows", 0),
        ])
    _write_csv(os.path.join(output_dir, "process_stats.csv"), process_header, process_rows)

    product_events = getattr(collector, "product_events", []) or []

    product_header = [
        "product_id",
        "arrival_time",
        "completion_time",
        "cycle_time_min",
        "defect_found",
    ]
    product_rows = []
    for item in product_events:
        if item.get("event_type") != "product_complete":
            continue
        product_rows.append([
            item.get("product_id", 0),
            item.get("arrival_time", 0),
            item.get("completion_time", 0),
            item.get("cycle_time", 0),
            item.get("defect_found", False),
        ])
    _write_csv(os.path.join(output_dir, "product_log.csv"), product_header, product_rows)

    stage_event_header = [
        "product_id",
        "stage_name",
        "stage_start",
        "stage_end",
        "wait_time_min",
        "process_time_min",
        "defect_occurred",
    ]
    stage_event_rows = []
    for item in product_events:
        if "event_type" in item:
            continue
        stage_event_rows.append([
            item.get("product_id", 0),
            item.get("stage_name", ""),
            item.get("stage_start", 0),
            item.get("stage_end", 0),
            item.get("wait_time", 0),
            item.get("process_time", 0),
            item.get("defect_occurred", False),
        ])
    _write_csv(os.path.join(output_dir, "stage_event_log.csv"), stage_event_header, stage_event_rows)

    checkpoint_log = getattr(collector, "checkpoint_log", []) or []
    checkpoint_process_names = set()
    for checkpoint in checkpoint_log:
        processes = checkpoint.get("processes", {})
        if isinstance(processes, dict):
            checkpoint_process_names.update(processes.keys())
    checkpoint_process_names = sorted(checkpoint_process_names)

    checkpoint_header = [
        "sim_time",
        "total_produced",
        "total_defects",
        "current_wip",
    ] + [f"{name}_interval_count" for name in checkpoint_process_names]
    checkpoint_rows = []
    for checkpoint in checkpoint_log:
        row = [
            checkpoint.get("sim_time", 0),
            checkpoint.get("total_produced", 0),
            checkpoint.get("total_defects", 0),
            checkpoint.get("current_wip", 0),
        ]
        processes = checkpoint.get("processes", {})
        for name in checkpoint_process_names:
            process_row = processes.get(name, {}) if isinstance(processes, dict) else {}
            row.append(process_row.get("interval_count", 0))
        checkpoint_rows.append(row)
    _write_csv(os.path.join(output_dir, "checkpoint_timeline.csv"), checkpoint_header, checkpoint_rows)

    cost_header = ["cost_type", "amount", "time", "stage_name"]
    cost_rows = []
    for item in getattr(collector, "cost_events", []) or []:
        cost_rows.append([
            item.get("cost_type", ""),
            item.get("amount", 0),
            item.get("time", 0),
            item.get("stage_name", "") or "",
        ])
    _write_csv(os.path.join(output_dir, "cost_log.csv"), cost_header, cost_rows)

    wip_header = ["time_min", "wip"]
    wip_rows = []
    for time_value, wip_value in getattr(collector, "wip_log", []) or []:
        wip_rows.append([time_value, wip_value])
    _write_csv(os.path.join(output_dir, "wip_log.csv"), wip_header, wip_rows)

    # 8. utilization.csv — 공정별 설비 가동률
    _write_csv(
        os.path.join(output_dir, "utilization.csv"),
        ["process_name", "utilization_rate", "availability_rate",
         "busy_time_min", "repair_time_min", "total_capacity_min"],
        [
            [name, u["utilization_rate"], u["availability_rate"],
             u["busy_time_min"], u["repair_time_min"], u["total_capacity_min"]]
            for name, u in utilization.items()
        ],
    )

    # 9. due_date_performance.csv — 납기 준수율
    _write_csv(
        os.path.join(output_dir, "due_date_performance.csv"),
        ["target_lead_time_min", "on_time_count", "total_count", "due_date_adherence_rate"],
        [[
            due_perf.get("target_lead_time_min", 0),
            due_perf.get("on_time_count", 0),
            due_perf.get("total_count", 0),
            due_perf.get("due_date_adherence_rate", 0),
        ]],
    )
