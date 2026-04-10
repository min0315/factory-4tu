# simulation.py
# 4TU 실측 파라미터 기반 SimPy 이산사건 시뮬레이션을 실행한다.

import math
import random
import threading

import numpy as np
import simpy

from collector import DataCollector


class LivePolicyStore:
    """실행 중 공정 파라미터를 안전하게 덮어쓰기 위한 저장소."""

    def __init__(self):
        self._lock = threading.Lock()
        self._overrides = {}

    def update(self, process_name, overrides):
        """특정 공정의 런타임 파라미터를 갱신한다."""
        with self._lock:
            self._overrides[process_name] = dict(overrides)

    def get(self, base_cfg):
        """기본 설정과 override를 병합한 현재 공정 설정을 반환한다."""
        with self._lock:
            override = dict(self._overrides.get(base_cfg["name"], {}))
        merged = dict(base_cfg)
        merged.update(override)
        return merged


def _sample_lognormal(mean_value, std_value, rng):
    """평균/표준편차를 만족하는 로그정규 분포에서 처리시간을 샘플링한다."""
    mean_value = max(0.1, float(mean_value))
    std_value = max(0.0, float(std_value))
    if std_value <= 1e-9:
        return mean_value

    variance = std_value ** 2
    sigma2 = math.log(1.0 + (variance / (mean_value ** 2)))
    sigma = math.sqrt(max(0.0, sigma2))
    mu = math.log(mean_value) - (sigma2 / 2.0)
    return max(0.1, rng.lognormvariate(mu, sigma))


def _build_process_index(config):
    """공정 이름 기준 설정 조회용 딕셔너리를 생성한다."""
    return {item["name"]: dict(item) for item in config["processes"]}


def _run_stage(env, product_id, stage_name, stage_cfg, resource, collector, policy_store, rng, costs):
    """제품이 단일 공정을 통과하는 과정을 시뮬레이션한다."""
    queue_enter = env.now
    collector.update_queue_length(stage_name, env.now, len(resource.queue))

    with resource.request() as req:
        yield req
        wait_time = env.now - queue_enter
        collector.update_queue_length(stage_name, env.now, len(resource.queue))

        live_cfg = policy_store.get(stage_cfg)
        pt_mean = float(live_cfg["process_time_mean"]) * float(
            live_cfg.get("process_time_multiplier", 1.0)
        )
        defect_rate = min(
            0.99,
            float(live_cfg.get("defect_rate", 0.0))
            * float(live_cfg.get("failure_rate_multiplier", 1.0)),
        )
        process_time = _sample_lognormal(
            pt_mean,
            live_cfg["process_time_std"],
            rng,
        )
        stage_start = env.now
        stage_end = stage_start + process_time
        defect_occurred = rng.random() < defect_rate

        yield env.timeout(process_time)
        collector.record_stage_event(
            product_id=product_id,
            stage_name=stage_name,
            machine_name=f"{stage_name}_resource",
            stage_start=stage_start,
            stage_end=stage_end,
            wait_time=wait_time,
            process_time=process_time,
            defect_occurred=defect_occurred,
        )
        collector.record_cost(
            cost_type="processing",
            amount=(costs["machine_cost_per_hour"] / 60.0) * process_time,
            time=stage_end,
            stage_name=stage_name,
        )
        collector.record_cost(
            cost_type="wip_holding",
            amount=costs["wip_holding_cost_per_minute"] * (wait_time + process_time),
            time=stage_end,
            stage_name=stage_name,
        )
        if defect_occurred:
            collector.record_cost(
                cost_type="defect",
                amount=costs["defect_cost_per_unit"],
                time=stage_end,
                stage_name=stage_name,
            )
        return defect_occurred


def _product_flow(
    env,
    product_id,
    routing,
    process_index,
    resources,
    collector,
    policy_store,
    rng,
    costs,
):
    """한 제품이 대표 라우팅의 모든 공정을 통과하도록 실행한다."""
    arrival_time = env.now
    collector.record_product_start(product_id, arrival_time, routing)
    defect_found = False

    for stage_name in routing:
        stage_cfg = process_index.get(stage_name)
        if not stage_cfg:
            continue
        stage_defect = yield env.process(
            _run_stage(
                env=env,
                product_id=product_id,
                stage_name=stage_name,
                stage_cfg=stage_cfg,
                resource=resources[stage_name],
                collector=collector,
                policy_store=policy_store,
                rng=rng,
                costs=costs,
            )
        )
        defect_found = defect_found or bool(stage_defect)

    collector.record_product_complete(product_id, arrival_time, env.now, defect_found)


def _arrival_process(env, config, process_index, resources, collector, policy_store, rng):
    """지수분포 도착 간격으로 케이스를 생성한다."""
    inter_arrival_mean = float(config["arrival"]["inter_arrival_mean"])
    routing = list(config.get("common_routing", []))
    costs = dict(config.get("costs", {}))
    product_id = 0

    while True:
        interval = max(0.1, rng.expovariate(1.0 / max(inter_arrival_mean, 0.1)))
        yield env.timeout(interval)
        product_id += 1
        env.process(
            _product_flow(
                env=env,
                product_id=product_id,
                routing=routing,
                process_index=process_index,
                resources=resources,
                collector=collector,
                policy_store=policy_store,
                rng=rng,
                costs=costs,
            )
        )


def run_simulation(config, agent_manager=None, checkpoint_minutes=3000):
    """SimPy 환경을 구성하고 종료 시 collector를 반환한다."""
    sim_cfg = config.get("sim", {})
    seed = int(sim_cfg.get("random_seed", 42))
    duration = float(sim_cfg.get("duration", 127000))

    random.seed(seed)
    np.random.seed(seed)
    rng = random.Random(seed)

    env = simpy.Environment()
    policy_store = LivePolicyStore()
    collector = DataCollector(config)
    process_index = _build_process_index(config)
    resources = {
        stage_name: simpy.Resource(env, capacity=max(1, int(stage_cfg["num_machines"])))
        for stage_name, stage_cfg in process_index.items()
    }

    env.process(
        _arrival_process(
            env=env,
            config=config,
            process_index=process_index,
            resources=resources,
            collector=collector,
            policy_store=policy_store,
            rng=rng,
        )
    )
    if agent_manager is None:
        env.run(until=duration)
        return collector

    agent_manager.set_policy_store(policy_store)
    checkpoint_minutes = max(1.0, float(checkpoint_minutes))
    while env.now < duration:
        next_checkpoint = min(duration, env.now + checkpoint_minutes)
        env.run(until=next_checkpoint)
        snapshot = collector.build_snapshot(env.now)
        agent_manager.evaluate_all(snapshot)
    return collector
