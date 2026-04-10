# config.py
# 4TU Production Analysis CSV를 읽어 시뮬레이션 설정을 자동 생성한다.

import os
from collections import Counter

import pandas as pd


# 출력 폴더와 같은 레벨이 아니라 현재 폴더 기준 한 단계 위의 데이터 경로를 사용한다.
CSV_PATH = os.path.join(
    os.path.dirname(__file__),
    "..",
    "4tu-production",
    "Production_Data.csv",
)

PROCESS_COLORS = {
    "Turning_Milling": "#58a6ff",
    "QC_Turning": "#f0883e",
    "Lapping": "#3fb950",
    "Round_Grinding": "#bc8cff",
    "Flat_Grinding": "#79c0ff",
    "Laser_Marking": "#ff7b72",
    "Final_Inspection": "#d29922",
    "Packing": "#56d364",
    "Heat_Treatment": "#8b949e",
    "Other": "#6e7681",
}

DEFAULT_COSTS = {
    "labor_cost_per_hour": 28000.0,
    "machine_cost_per_hour": 42000.0,
    "wip_holding_cost_per_minute": 6.0,
    "defect_cost_per_unit": 90000.0,
}


def get_process_type(activity):
    """Activity 문자열을 표준 공정 유형으로 매핑한다."""
    a = str(activity).lower()
    if "turning" in a and "q.c" in a:
        return "QC_Turning"
    if "turning" in a:
        return "Turning_Milling"
    if "lapping" in a:
        return "Lapping"
    if "round grinding" in a:
        return "Round_Grinding"
    if "flat grinding" in a:
        return "Flat_Grinding"
    if "laser" in a:
        return "Laser_Marking"
    if "final inspection" in a:
        return "Final_Inspection"
    if "packing" in a:
        return "Packing"
    if "heat treat" in a:
        return "Heat_Treatment"
    return "Other"


def _load_raw_dataframe():
    """실제 CSV를 읽고 시간 컬럼과 파생 컬럼을 정리한다."""
    df = pd.read_csv(CSV_PATH)
    df["Start Timestamp"] = pd.to_datetime(df["Start Timestamp"])
    df["Complete Timestamp"] = pd.to_datetime(df["Complete Timestamp"])
    df["process_type"] = df["Activity"].map(get_process_type)
    df["process_minutes"] = (
        df["Complete Timestamp"] - df["Start Timestamp"]
    ).dt.total_seconds() / 60.0
    df["Qty Rejected"] = pd.to_numeric(df["Qty Rejected"], errors="coerce").fillna(0.0)
    df["Qty Completed"] = pd.to_numeric(df["Qty Completed"], errors="coerce").fillna(0.0)
    df["Work Order  Qty"] = pd.to_numeric(
        df["Work Order  Qty"],
        errors="coerce",
    ).fillna(0.0)
    return df


def _dedupe_consecutive(seq):
    """연속 중복 공정을 제거해 라우팅을 정리한다."""
    cleaned = []
    for item in seq:
        if not cleaned or cleaned[-1] != item:
            cleaned.append(item)
    return cleaned


def _collapse_rework(seq):
    """같은 공정이 재방문된 경우 최초 방문만 남겨 대표 라우팅 후보를 만든다."""
    collapsed = []
    seen = set()
    for item in _dedupe_consecutive(seq):
        if item in seen:
            continue
        collapsed.append(item)
        seen.add(item)
    return collapsed


def _build_routing(df):
    """케이스별 라우팅과 가장 빈번한 대표 라우팅을 계산한다."""
    routing = {}
    routing_for_pattern = {}
    for case_id, case_df in df.sort_values(
        ["Case ID", "Start Timestamp", "Complete Timestamp"]
    ).groupby("Case ID"):
        sequence = [
            item
            for item in case_df["process_type"].tolist()
            if item != "Other"
        ]
        routing[str(case_id)] = _dedupe_consecutive(sequence)
        routing_for_pattern[str(case_id)] = _collapse_rework(sequence)

    routing_counter = Counter(tuple(route) for route in routing_for_pattern.values() if route)
    common_routing = list(routing_counter.most_common(1)[0][0]) if routing_counter else []
    return routing, common_routing, routing_counter


def _build_process_configs(df):
    """공정별 실측 평균, 표준편차, 기계 수, 불량률을 생성한다."""
    process_rows = []
    real_process_stats = {}

    filtered = df[df["process_type"] != "Other"].copy()
    for process_name, group in filtered.groupby("process_type"):
        mean_minutes = float(group["process_minutes"].mean())
        std_minutes = float(group["process_minutes"].std(ddof=1) or 0.0)
        num_machines = int(group["Resource"].nunique())
        # 행 기준(이벤트 발생 비율)이 아닌 단위 기준(불량 수량/전체 수량)으로 계산
        total_qty = group["Qty Completed"].sum() + group["Qty Rejected"].sum()
        defect_rate = float(group["Qty Rejected"].sum() / total_qty) if total_qty > 0 else 0.0

        process_rows.append(
            {
                "name": process_name,
                "label": process_name,
                "process_time_mean": round(mean_minutes, 4),
                "process_time_std": round(std_minutes, 4),
                "num_machines": num_machines,
                "defect_rate": round(defect_rate, 4),
                "color": PROCESS_COLORS.get(process_name, "#6e7681"),
            }
        )

        real_process_stats[process_name] = {
            "mean_minutes": round(mean_minutes, 4),
            "std_minutes": round(std_minutes, 4),
            "num_machines": num_machines,
            "defect_rate": round(defect_rate, 4),
            "throughput_rows": int(len(group)),
        }

    process_rows.sort(key=lambda item: item["name"])
    return process_rows, real_process_stats


def _build_data_summary(df, common_routing, routing_counter, real_process_stats):
    """실측 데이터 비교용 요약 정보를 구성한다."""
    case_starts = df.groupby("Case ID")["Start Timestamp"].min().sort_values()
    case_completes = df.groupby("Case ID")["Complete Timestamp"].max().sort_values()
    inter_arrivals = case_starts.diff().dropna().dt.total_seconds() / 60.0
    start_min = df["Start Timestamp"].min()
    end_max = df["Complete Timestamp"].max()
    duration_minutes = (end_max - start_min).total_seconds() / 60.0

    return {
        "row_count": int(len(df)),
        "case_count": int(df["Case ID"].nunique()),
        "part_count": int(df["Part Desc."].nunique()),
        "process_count": int(df["process_type"].nunique() - int((df["process_type"] == "Other").any())),
        "report_types": sorted(df["Report Type"].dropna().astype(str).unique().tolist()),
        "start_date": start_min.strftime("%Y-%m-%d"),
        "end_date": end_max.strftime("%Y-%m-%d"),
        "duration_minutes": round(duration_minutes, 2),
        "avg_inter_arrival_minutes": round(float(inter_arrivals.mean()), 4),
        "std_inter_arrival_minutes": round(float(inter_arrivals.std(ddof=1)), 4),
        "expected_arrivals_for_duration_127000": round(127000.0 / float(inter_arrivals.mean()), 2),
        "avg_work_order_qty": round(float(df["Work Order  Qty"].mean()), 4),
        "avg_qty_completed": round(float(df["Qty Completed"].mean()), 4),
        "avg_qty_rejected": round(float(df["Qty Rejected"].mean()), 4),
        "completed_case_count": int((case_completes.notna()).sum()),
        "common_routing_frequency": int(routing_counter.get(tuple(common_routing), 0)),
        "common_routing": list(common_routing),
        "real_process_stats": real_process_stats,
    }


def _build_factory_config():
    """전체 FACTORY_CONFIG를 생성한다."""
    df = _load_raw_dataframe()
    processes, real_process_stats = _build_process_configs(df)
    routing, common_routing, routing_counter = _build_routing(df)
    data_summary = _build_data_summary(df, common_routing, routing_counter, real_process_stats)

    return {
        "data_source": CSV_PATH,
        "processes": processes,
        "routing": routing,
        "common_routing": common_routing,
        "arrival": {"inter_arrival_mean": 567.5},
        "sim": {"duration": 127000, "random_seed": 42},
        "costs": DEFAULT_COSTS,
        "data_summary": data_summary,
    }


REAL_DATA_DF = _load_raw_dataframe()
FACTORY_CONFIG = _build_factory_config()
