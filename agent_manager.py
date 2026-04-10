# agent_manager.py
# 공정별 에이전트를 병렬 실행하고 정책 override를 적용한다.

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed

from agents.process_agent import (
    FinalInspectionAgent,
    FlatGrindingAgent,
    LaserMarkingAgent,
    LappingAgent,
    ProcessKPI,
    QCTurningAgent,
    RoundGrindingAgent,
    TurningMillingAgent,
)


STATE_DIR = "/Users/minseong/claude/아이디어톤/factory-4tu/state"


class AgentManager:
    """체크포인트마다 모든 공정 에이전트를 실행한다."""

    def __init__(self, checkpoint_minutes):
        self.checkpoint_minutes = checkpoint_minutes
        self.policy_store = None
        self.agents = [
            TurningMillingAgent("Turning_Milling", f"{STATE_DIR}/Turning_Milling_agent.json"),
            QCTurningAgent("QC_Turning", f"{STATE_DIR}/QC_Turning_agent.json"),
            LappingAgent("Lapping", f"{STATE_DIR}/Lapping_agent.json"),
            RoundGrindingAgent("Round_Grinding", f"{STATE_DIR}/Round_Grinding_agent.json"),
            FlatGrindingAgent("Flat_Grinding", f"{STATE_DIR}/Flat_Grinding_agent.json"),
            LaserMarkingAgent("Laser_Marking", f"{STATE_DIR}/Laser_Marking_agent.json"),
            FinalInspectionAgent("Final_Inspection", f"{STATE_DIR}/Final_Inspection_agent.json"),
        ]

    def set_policy_store(self, policy_store):
        """런타임 policy store를 연결한다."""
        self.policy_store = policy_store

    def evaluate_all(self, snapshot):
        """모든 에이전트를 병렬 평가하고 override를 즉시 반영한다."""
        if self.policy_store is None:
            raise RuntimeError("policy_store가 설정되지 않았습니다.")

        agent_scores = {}
        agent_controls = {}
        with ThreadPoolExecutor(max_workers=7) as executor:
            future_map = {
                executor.submit(
                    agent.decide,
                    self._build_kpi(snapshot, agent.process_name),
                    self.checkpoint_minutes,
                ): agent
                for agent in self.agents
            }
            for future in as_completed(future_map):
                agent = future_map[future]
                controls = future.result()
                overrides = agent.get_runtime_overrides()
                self.policy_store.update(agent.process_name, overrides)
                agent_scores[agent.process_name] = agent.last_score
                agent_controls[agent.process_name] = dict(controls)

        snapshot["agent_scores"] = agent_scores
        snapshot["agent_controls"] = agent_controls
        return agent_controls

    def print_policy_summary(self):
        """시뮬레이션 종료 후 최종 정책을 출력한다."""
        print("=" * 60)
        print("LLM Agent Policy Summary")
        print("=" * 60)
        for agent in self.agents:
            print(f"{agent.process_name:18s} | score={agent.last_score:7.3f} | {agent.current_policy}")
        print("=" * 60)

    def _build_kpi(self, snapshot, process_name):
        """스냅샷에서 개별 에이전트용 KPI를 생성한다."""
        process_data = snapshot.get("processes", {}).get(process_name, {})
        return ProcessKPI(
            sim_time=float(snapshot.get("sim_time", 0.0)),
            interval_count=int(process_data.get("interval_count", 0)),
            interval_failures=int(process_data.get("interval_failures", 0)),
            interval_repair_min=float(process_data.get("interval_repair_minutes", 0.0)),
            avg_proc_time=float(process_data.get("avg_proc_time", 0.0)),
            avg_wait_time=float(process_data.get("avg_wait_time", 0.0)),
            queue_length=int(process_data.get("queue_length", 0)),
        )
