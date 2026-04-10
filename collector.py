# collector.py
# 시뮬레이션 중 발생한 제품, 공정, 비용, WIP 이벤트를 집계한다.

from collections import defaultdict

import numpy as np


class DataCollector:
    """리포트와 실측 비교에 필요한 이벤트를 모두 수집한다."""

    def __init__(self, config):
        self.config = config
        self.product_events = []
        self.process_names = [item["name"] for item in config.get("processes", [])]
        self.stage_stats = defaultdict(
            lambda: {
                "entries": 0,
                "completed": 0,
                "total_wait_time": 0.0,
                "total_process_time": 0.0,
                "defects": 0,
                "queue_area": 0.0,
                "last_queue_time": 0.0,
                "last_queue_length": 0,
            }
        )
        self.interval_stage_stats = defaultdict(
            lambda: {
                "count": 0,
                "failures": 0,
                "repair_minutes": 0.0,
                "total_wait_time": 0.0,
                "total_process_time": 0.0,
            }
        )
        self.wip_log = [(0.0, 0)]
        self.cost_events = []
        self.current_wip = 0
        self.total_completed = 0
        self.total_defects = 0
        self.checkpoint_log = []

    def record_wip_change(self, time, delta):
        """WIP 변화를 시계열로 저장한다."""
        self.current_wip = max(0, self.current_wip + int(delta))
        self.wip_log.append((float(time), self.current_wip))

    def update_queue_length(self, stage_name, time, queue_length):
        """공정별 대기열 면적을 적분 방식으로 누적한다."""
        stats = self.stage_stats[stage_name]
        now = float(time)
        elapsed = max(0.0, now - stats["last_queue_time"])
        stats["queue_area"] += stats["last_queue_length"] * elapsed
        stats["last_queue_time"] = now
        stats["last_queue_length"] = int(queue_length)

    def record_stage_event(
        self,
        product_id,
        stage_name,
        machine_name,
        stage_start,
        stage_end,
        wait_time,
        process_time,
        defect_occurred,
    ):
        """공정 처리 결과를 제품 이벤트와 공정 통계에 함께 반영한다."""
        stats = self.stage_stats[stage_name]
        stats["entries"] += 1
        stats["completed"] += 1
        stats["total_wait_time"] += float(wait_time)
        stats["total_process_time"] += float(process_time)
        if defect_occurred:
            stats["defects"] += 1

        interval = self.interval_stage_stats[stage_name]
        interval["count"] += 1
        interval["total_wait_time"] += float(wait_time)
        interval["total_process_time"] += float(process_time)
        if defect_occurred:
            interval["failures"] += 1

        event = {
            "product_id": int(product_id),
            "stage_name": stage_name,
            "machine_name": machine_name,
            "stage_start": float(stage_start),
            "stage_end": float(stage_end),
            "wait_time": float(wait_time),
            "process_time": float(process_time),
            "defect_occurred": bool(defect_occurred),
        }
        self.product_events.append(event)

    def record_product_start(self, product_id, arrival_time, routing):
        """제품 도착 이벤트를 기록한다."""
        self.record_wip_change(arrival_time, 1)
        self.product_events.append(
            {
                "product_id": int(product_id),
                "event_type": "product_start",
                "arrival_time": float(arrival_time),
                "routing": list(routing),
            }
        )

    def record_product_complete(self, product_id, arrival_time, completion_time, defect_found):
        """제품 완료 이벤트를 기록한다."""
        self.record_wip_change(completion_time, -1)
        self.total_completed += 1
        if defect_found:
            self.total_defects += 1
        self.product_events.append(
            {
                "product_id": int(product_id),
                "event_type": "product_complete",
                "arrival_time": float(arrival_time),
                "completion_time": float(completion_time),
                "cycle_time": float(completion_time - arrival_time),
                "defect_found": bool(defect_found),
            }
        )

    def record_cost(self, cost_type, amount, time, stage_name=None):
        """비용 이벤트를 누적한다."""
        self.cost_events.append(
            {
                "cost_type": str(cost_type),
                "amount": float(amount),
                "time": float(time),
                "stage_name": stage_name,
            }
        )

    def build_snapshot(self, sim_time):
        """체크포인트 시점의 구간 KPI 스냅샷을 생성하고 구간 누적치를 초기화한다."""
        processes = {}
        for stage_name in self.process_names:
            interval = self.interval_stage_stats[stage_name]
            queue_length = int(self.stage_stats[stage_name]["last_queue_length"])
            count = int(interval["count"])
            processes[stage_name] = {
                "interval_count": count,
                "interval_failures": int(interval["failures"]),
                "interval_repair_minutes": round(float(interval["repair_minutes"]), 4),
                "avg_proc_time": round(
                    float(interval["total_process_time"] / count) if count else 0.0,
                    4,
                ),
                "avg_wait_time": round(
                    float(interval["total_wait_time"] / count) if count else 0.0,
                    4,
                ),
                "queue_length": queue_length,
            }
            self.interval_stage_stats[stage_name] = {
                "count": 0,
                "failures": 0,
                "repair_minutes": 0.0,
                "total_wait_time": 0.0,
                "total_process_time": 0.0,
            }

        snapshot = {
            "sim_time": float(sim_time),
            "total_produced": int(self.total_completed),
            "total_defects": int(self.total_defects),
            "current_wip": int(self.current_wip),
            "processes": processes,
        }
        self.checkpoint_log.append(snapshot)
        return snapshot

    def _product_completions(self):
        """완료된 제품 이벤트만 분리한다."""
        return [
            item
            for item in self.product_events
            if item.get("event_type") == "product_complete"
        ]

    def _stage_event_rows(self):
        """공정 이벤트만 분리한다."""
        return [
            item
            for item in self.product_events
            if "stage_name" in item
        ]

    def get_summary(self):
        """전체 KPI와 공정 요약을 반환한다."""
        completions = self._product_completions()
        stage_rows = self._stage_event_rows()
        duration = float(self.config.get("sim", {}).get("duration", 0.0))

        total_cost = sum(item["amount"] for item in self.cost_events)
        throughput = len(completions)
        defect_count = sum(1 for item in completions if item["defect_found"])
        cycle_times = [item["cycle_time"] for item in completions]

        stage_summary = {}
        for stage_name, stats in self.stage_stats.items():
            avg_wait = stats["total_wait_time"] / stats["completed"] if stats["completed"] else 0.0
            avg_process = stats["total_process_time"] / stats["completed"] if stats["completed"] else 0.0
            defect_rate = stats["defects"] / stats["completed"] if stats["completed"] else 0.0
            queue_area = stats["queue_area"] + (
                stats["last_queue_length"] * max(0.0, duration - stats["last_queue_time"])
            )
            avg_queue = queue_area / duration if duration > 0 else 0.0
            stage_summary[stage_name] = {
                "completed": int(stats["completed"]),
                "avg_wait_time": round(avg_wait, 4),
                "avg_process_time": round(avg_process, 4),
                "defect_rate": round(defect_rate, 4),
                "avg_queue_length": round(avg_queue, 4),
            }

        return {
            "throughput": throughput,
            "defect_count": defect_count,
            "defect_rate": round(defect_count / throughput, 4) if throughput else 0.0,
            "avg_cycle_time": round(float(np.mean(cycle_times)), 4) if cycle_times else 0.0,
            "median_cycle_time": round(float(np.median(cycle_times)), 4) if cycle_times else 0.0,
            "max_cycle_time": round(float(np.max(cycle_times)), 4) if cycle_times else 0.0,
            "avg_wip": round(self._avg_wip(duration), 4),
            "total_cost": round(total_cost, 4),
            "stage_stats": stage_summary,
            "product_count_started": len(
                [item for item in self.product_events if item.get("event_type") == "product_start"]
            ),
            "stage_event_count": len(stage_rows),
        }

    def _avg_wip(self, duration):
        """WIP 평균을 면적 기준으로 계산한다."""
        if duration <= 0 or len(self.wip_log) < 2:
            return 0.0
        area = 0.0
        for idx in range(1, len(self.wip_log)):
            prev_time, prev_wip = self.wip_log[idx - 1]
            now_time, _ = self.wip_log[idx]
            area += max(0.0, now_time - prev_time) * prev_wip
        last_time, last_wip = self.wip_log[-1]
        area += max(0.0, duration - last_time) * last_wip
        return area / duration

    def compare_with_real(self, real_df):
        """실측 데이터와 시뮬레이션 요약을 비교한다."""
        sim_summary = self.get_summary()
        process_compare = {}
        real_stats = self.config.get("data_summary", {}).get("real_process_stats", {})

        for process_name, real in real_stats.items():
            sim_stage = sim_summary["stage_stats"].get(process_name, {})
            process_compare[process_name] = {
                "real_process_time_mean": round(float(real.get("mean_minutes", 0.0)), 4),
                "sim_process_time_mean": round(float(sim_stage.get("avg_process_time", 0.0)), 4),
                "real_defect_rate": round(float(real.get("defect_rate", 0.0)), 4),
                "sim_defect_rate": round(float(sim_stage.get("defect_rate", 0.0)), 4),
                "real_throughput_rows": int(real.get("throughput_rows", 0)),
                "sim_throughput_rows": int(sim_stage.get("completed", 0)),
            }

        real_case_count = int(real_df["Case ID"].nunique())
        real_arrival_mean = float(
            self.config.get("data_summary", {}).get("avg_inter_arrival_minutes", 0.0)
        )

        return {
            "real_case_count": real_case_count,
            "sim_started_cases": int(sim_summary["product_count_started"]),
            "sim_completed_cases": int(sim_summary["throughput"]),
            "real_avg_inter_arrival_minutes": round(real_arrival_mean, 4),
            "sim_avg_cycle_time": sim_summary["avg_cycle_time"],
            "common_routing": list(self.config.get("common_routing", [])),
            "process_comparison": process_compare,
        }

    def get_utilization(self, config):
        """
        공정별 설비 가동률을 계산한다.
        가동률 = 총 처리 시간 / (duration × num_machines)
        factory-4tu는 고장 모델이 없으므로 가용률 = 1.0
        """
        duration = float(config.get("sim", {}).get("duration", 0.0))
        proc_lookup = {p["name"]: p for p in config["processes"]}
        result = {}
        for stage_name, stats in self.stage_stats.items():
            proc = proc_lookup.get(stage_name, {})
            num_machines = int(proc.get("num_machines", 1))
            total_capacity = duration * num_machines
            busy = stats["total_process_time"]
            result[stage_name] = {
                "utilization_rate": round(busy / total_capacity, 4) if total_capacity > 0 else 0.0,
                "availability_rate": 1.0,
                "busy_time_min": round(busy, 2),
                "repair_time_min": 0.0,
                "total_capacity_min": round(total_capacity, 2),
            }
        return result

    def get_due_date_performance(self, config):
        """
        납기 준수율을 계산한다.
        목표 납기 = 공정별 process_time_mean 합계 × 1.5 (여유율 50%)
        실제 리드타임 = product_complete 이벤트의 cycle_time
        """
        total_mean = sum(p["process_time_mean"] for p in config["processes"])
        target_lead_time = total_mean * 1.5
        completions = self._product_completions()
        on_time = sum(1 for c in completions if c["cycle_time"] <= target_lead_time)
        total = len(completions)
        return {
            "target_lead_time_min": round(target_lead_time, 2),
            "on_time_count": on_time,
            "total_count": total,
            "due_date_adherence_rate": round(on_time / total, 4) if total > 0 else 0.0,
        }
