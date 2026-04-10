# agents/process_agent.py
# 공정별 Ollama LLM 에이전트와 정책 변환 로직을 정의한다.

from __future__ import annotations

import json
import os
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from urllib import error, request


OLLAMA_ENDPOINT = "http://localhost:11434/api/chat"
OLLAMA_MODEL = "qwen2.5:7b"
REQUEST_TIMEOUT = 60


@dataclass
class ProcessKPI:
    """체크포인트 구간 KPI를 전달하기 위한 데이터 구조다."""

    sim_time: float
    interval_count: int
    interval_failures: int
    interval_repair_min: float
    avg_proc_time: float
    avg_wait_time: float
    queue_length: int


class BaseProcessAgent(ABC):
    """단일 공정을 제어하는 기본 LLM 에이전트다."""

    def __init__(self, process_name, state_path):
        self.process_name = process_name
        self.state_path = state_path
        self.current_policy = self._default_policy()
        loaded_policy = self._load_policy()
        self.current_policy = loaded_policy
        self.last_score = 0.0

    @property
    @abstractmethod
    def bounds(self):
        """제어 가능한 파라미터의 허용 범위를 반환한다."""

    @property
    @abstractmethod
    def system_prompt(self):
        """모델에 전달할 시스템 프롬프트를 반환한다."""

    def score(self, kpi, checkpoint_minutes):
        """구간 처리 성능을 단일 점수로 환산한다."""
        throughput_per_hour = (
            float(kpi.interval_count) * 60.0 / max(float(checkpoint_minutes), 1.0)
        )
        return (
            throughput_per_hour
            - 2.0 * float(kpi.interval_failures)
            - 0.05 * float(kpi.avg_wait_time)
            - 0.1 * float(kpi.queue_length)
        )

    def decide(self, kpi, checkpoint_minutes):
        """현재 KPI를 바탕으로 다음 제어값을 결정한다."""
        self.last_score = round(float(self.score(kpi, checkpoint_minutes)), 4)
        payload = {
            "process_name": self.process_name,
            "checkpoint_minutes": int(checkpoint_minutes),
            "score": self.last_score,
            "current_policy": dict(self.current_policy),
            "kpi": asdict(kpi),
            "bounds": {
                name: {"min": limits[0], "max": limits[1]}
                for name, limits in self.bounds.items()
            },
        }
        try:
            decision = self._call_llm(payload)
            validated = self._validate(decision)
            self.current_policy = validated
            self._save_policy(validated)
        except Exception as exc:
            print(f"[Agent:{self.process_name}] LLM 실패로 기존 정책 유지: {exc}")
        return dict(self.current_policy)

    def get_runtime_overrides(self):
        """시뮬레이션 런타임 override 형식으로 정책을 변환한다."""
        return self._controls_to_overrides(self.current_policy)

    @abstractmethod
    def _controls_to_overrides(self, controls):
        """제어값을 시뮬레이터 파라미터 override로 변환한다."""

    def _call_llm(self, payload):
        """Ollama chat API를 호출해 JSON 응답을 파싱한다."""
        body = {
            "model": OLLAMA_MODEL,
            "stream": False,
            "format": "json",
            "messages": [
                {"role": "system", "content": self.system_prompt},
                {
                    "role": "user",
                    "content": json.dumps(payload, ensure_ascii=False),
                },
            ],
        }
        req = request.Request(
            OLLAMA_ENDPOINT,
            data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                raw = resp.read().decode("utf-8")
        except error.URLError as exc:
            raise RuntimeError(str(exc)) from exc

        response_json = json.loads(raw)
        content = response_json.get("message", {}).get("content", "").strip()
        if not content:
            raise ValueError("빈 응답")
        parsed = json.loads(content)
        if not isinstance(parsed, dict):
            raise ValueError("JSON 객체가 아님")
        return parsed

    def _validate(self, decision):
        """모델 출력값을 허용 범위 안으로 보정한다."""
        validated = {}
        defaults = self._default_policy()
        for name, limits in self.bounds.items():
            base_value = self.current_policy.get(name, defaults[name])
            raw_value = decision.get(name, base_value)
            try:
                numeric_value = float(raw_value)
            except (TypeError, ValueError):
                numeric_value = float(base_value)
            validated[name] = round(min(limits[1], max(limits[0], numeric_value)), 4)
        return validated

    def _default_policy(self):
        """초기 정책은 각 파라미터 범위의 중앙값으로 설정한다."""
        return {
            name: round((limits[0] + limits[1]) / 2.0, 4)
            for name, limits in self.bounds.items()
        }

    def _load_policy(self):
        """저장된 정책이 있으면 읽고 없으면 기본 정책을 사용한다."""
        os.makedirs(os.path.dirname(self.state_path), exist_ok=True)
        if not os.path.exists(self.state_path):
            return self._default_policy()
        try:
            with open(self.state_path, "r", encoding="utf-8") as file_obj:
                data = json.load(file_obj)
            if isinstance(data, dict):
                return self._validate(data)
        except (OSError, json.JSONDecodeError) as exc:
            print(f"[Agent:{self.process_name}] 상태 파일 읽기 실패: {exc}")
        return self._default_policy()

    def _save_policy(self, controls):
        """현재 정책을 상태 파일에 저장한다."""
        os.makedirs(os.path.dirname(self.state_path), exist_ok=True)
        with open(self.state_path, "w", encoding="utf-8") as file_obj:
            json.dump(controls, file_obj, ensure_ascii=False, indent=2)


class TurningMillingAgent(BaseProcessAgent):
    @property
    def bounds(self):
        return {
            "spindle_speed_scale": (0.85, 1.15),
            "feed_rate_scale": (0.9, 1.2),
            "coolant_scale": (0.8, 1.2),
            "maintenance_bias": (0.0, 1.0),
        }

    @property
    def system_prompt(self):
        return (
            "당신은 Turning_Milling 공정 제어 에이전트다. "
            "반드시 JSON 객체만 반환하고 키는 spindle_speed_scale, feed_rate_scale, "
            "coolant_scale, maintenance_bias 만 포함한다."
        )

    def _controls_to_overrides(self, controls):
        spindle = controls["spindle_speed_scale"]
        feed = controls["feed_rate_scale"]
        coolant = controls["coolant_scale"]
        maintenance = controls["maintenance_bias"]
        return {
            "process_time_multiplier": round(1.0 / (0.55 * spindle + 0.45 * feed), 4),
            "failure_rate_multiplier": round(
                1.0
                + 0.7 * (spindle - 1.0)
                + 0.45 * (feed - 1.0)
                - 0.35 * (coolant - 1.0)
                - 0.3 * maintenance,
                4,
            ),
        }


class QCTurningAgent(BaseProcessAgent):
    @property
    def bounds(self):
        return {
            "inspection_speed_scale": (0.8, 1.3),
            "sampling_rate": (0.5, 1.0),
            "sensitivity": (0.7, 1.5),
        }

    @property
    def system_prompt(self):
        return (
            "당신은 QC_Turning 공정 제어 에이전트다. "
            "반드시 JSON 객체만 반환하고 키는 inspection_speed_scale, sampling_rate, sensitivity 만 포함한다."
        )

    def _controls_to_overrides(self, controls):
        sensitivity = controls["sensitivity"]
        return {
            "process_time_multiplier": round(controls["inspection_speed_scale"], 4),
            "failure_rate_multiplier": round(max(0.5, 1.0 / max(0.01, sensitivity)), 4),
        }


class LappingAgent(BaseProcessAgent):
    @property
    def bounds(self):
        return {
            "abrasive_grade": (0.8, 1.2),
            "pressure_scale": (0.85, 1.15),
            "cycle_time_scale": (0.8, 1.2),
        }

    @property
    def system_prompt(self):
        return (
            "당신은 Lapping 공정 제어 에이전트다. "
            "반드시 JSON 객체만 반환하고 키는 abrasive_grade, pressure_scale, cycle_time_scale 만 포함한다."
        )

    def _controls_to_overrides(self, controls):
        pressure = controls["pressure_scale"]
        abrasive = controls["abrasive_grade"]
        return {
            "process_time_multiplier": round(
                controls["cycle_time_scale"] / max(0.7, pressure),
                4,
            ),
            "failure_rate_multiplier": round(
                1.0 + 0.5 * (pressure - 1.0) - 0.3 * (abrasive - 1.0),
                4,
            ),
        }


class RoundGrindingAgent(BaseProcessAgent):
    @property
    def bounds(self):
        return {
            "wheel_speed_scale": (0.85, 1.15),
            "depth_of_cut_scale": (0.85, 1.15),
            "dressing_freq": (0.0, 1.0),
            "coolant_scale": (0.8, 1.2),
        }

    @property
    def system_prompt(self):
        return (
            "당신은 Round_Grinding 공정 제어 에이전트다. "
            "반드시 JSON 객체만 반환하고 키는 wheel_speed_scale, depth_of_cut_scale, dressing_freq, coolant_scale 만 포함한다."
        )

    def _controls_to_overrides(self, controls):
        wheel = controls["wheel_speed_scale"]
        depth = controls["depth_of_cut_scale"]
        dressing = controls["dressing_freq"]
        coolant = controls["coolant_scale"]
        return {
            "process_time_multiplier": round(1.0 / (0.5 * wheel + 0.5 * depth), 4),
            "failure_rate_multiplier": round(
                1.0
                + 0.6 * (wheel - 1.0)
                + 0.5 * (depth - 1.0)
                - 0.3 * dressing
                - 0.25 * (coolant - 1.0),
                4,
            ),
        }


class FlatGrindingAgent(BaseProcessAgent):
    @property
    def bounds(self):
        return {
            "table_speed_scale": (0.85, 1.15),
            "infeed_scale": (0.85, 1.15),
            "spark_out_time_scale": (0.7, 1.2),
        }

    @property
    def system_prompt(self):
        return (
            "당신은 Flat_Grinding 공정 제어 에이전트다. "
            "반드시 JSON 객체만 반환하고 키는 table_speed_scale, infeed_scale, spark_out_time_scale 만 포함한다."
        )

    def _controls_to_overrides(self, controls):
        table = controls["table_speed_scale"]
        infeed = controls["infeed_scale"]
        return {
            "process_time_multiplier": round(
                controls["spark_out_time_scale"] / max(0.7, 0.5 * table + 0.5 * infeed),
                4,
            ),
            "failure_rate_multiplier": round(
                1.0 + 0.5 * (table - 1.0) + 0.5 * (infeed - 1.0),
                4,
            ),
        }


class LaserMarkingAgent(BaseProcessAgent):
    @property
    def bounds(self):
        return {
            "laser_power_scale": (0.85, 1.15),
            "scan_speed_scale": (0.9, 1.2),
            "focus_adjust": (0.0, 1.0),
        }

    @property
    def system_prompt(self):
        return (
            "당신은 Laser_Marking 공정 제어 에이전트다. "
            "반드시 JSON 객체만 반환하고 키는 laser_power_scale, scan_speed_scale, focus_adjust 만 포함한다."
        )

    def _controls_to_overrides(self, controls):
        power = controls["laser_power_scale"]
        focus = controls["focus_adjust"]
        return {
            "process_time_multiplier": round(1.0 / max(0.7, controls["scan_speed_scale"]), 4),
            "failure_rate_multiplier": round(1.0 + 0.3 * (power - 1.0) - 0.2 * focus, 4),
        }


class FinalInspectionAgent(BaseProcessAgent):
    @property
    def bounds(self):
        return {
            "inspection_depth_scale": (0.8, 1.3),
            "auto_reject_threshold": (0.5, 1.0),
            "sensitivity": (0.7, 1.5),
        }

    @property
    def system_prompt(self):
        return (
            "당신은 Final_Inspection 공정 제어 에이전트다. "
            "반드시 JSON 객체만 반환하고 키는 inspection_depth_scale, auto_reject_threshold, sensitivity 만 포함한다."
        )

    def score(self, kpi, checkpoint_minutes):
        throughput_per_hour = (
            float(kpi.interval_count) * 60.0 / max(float(checkpoint_minutes), 1.0)
        )
        return throughput_per_hour - 3.0 * float(kpi.interval_failures) - 0.1 * float(kpi.queue_length)

    def _controls_to_overrides(self, controls):
        sensitivity = controls["sensitivity"]
        return {
            "process_time_multiplier": round(controls["inspection_depth_scale"], 4),
            "failure_rate_multiplier": round(max(0.3, 1.0 / max(0.01, sensitivity)), 4),
        }
