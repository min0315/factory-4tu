# report.py
# 시뮬레이션 KPI를 출력하고 실측 비교 차트를 생성한다.

import os
import platform

os.environ.setdefault("MPLCONFIGDIR", os.path.join("/tmp", "mplconfig_factory_4tu"))

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


# 운영체제별 한글 폰트를 분기한다.
if platform.system() == "Darwin":
    matplotlib.rcParams["font.family"] = "AppleGothic"
elif platform.system() == "Windows":
    matplotlib.rcParams["font.family"] = "Malgun Gothic"
else:
    matplotlib.rcParams["font.family"] = "NanumGothic"
matplotlib.rcParams["axes.unicode_minus"] = False


def _build_process_lookup(config):
    """공정 정의를 이름 기준으로 빠르게 조회한다."""
    return {item["name"]: item for item in config["processes"]}


def generate_report(collector, config, output_dir):
    """콘솔 KPI와 비교 차트 5개를 생성한다."""
    os.makedirs(output_dir, exist_ok=True)
    summary = collector.get_summary()
    process_lookup = _build_process_lookup(config)
    real_stats = config.get("data_summary", {}).get("real_process_stats", {})

    print("=" * 60)
    print("4TU Production Simulation KPI")
    print("=" * 60)
    print(f"시뮬레이션 기간(분)        : {config['sim']['duration']:,}")
    print(f"도착 케이스 수(시작)       : {summary['product_count_started']:,}")
    print(f"완료 케이스 수             : {summary['throughput']:,}")
    print(f"평균 사이클 타임(분)       : {summary['avg_cycle_time']:.2f}")
    print(f"평균 WIP                   : {summary['avg_wip']:.2f}")
    print(f"불량률                     : {summary['defect_rate'] * 100:.2f}%")
    print(f"총 비용                    : {summary['total_cost']:,.0f}")
    print(f"대표 라우팅                : {' -> '.join(config.get('common_routing', []))}")
    print(f"실측 케이스 수             : {config['data_summary']['case_count']:,}")
    print(f"실측 평균 도착간격(분)     : {config['data_summary']['avg_inter_arrival_minutes']:.2f}")
    print("=" * 60)

    utilization = collector.get_utilization(config)
    due_perf = collector.get_due_date_performance(config)

    print(f"\n[설비 가동률]")
    for stage_name, u in utilization.items():
        print(f"  {stage_name:20}: 가동률 {u['utilization_rate']*100:.1f}%  가용률 {u['availability_rate']*100:.1f}%")

    print(f"\n[납기 준수율]")
    print(f"  목표 납기       : {due_perf['target_lead_time_min']:.1f} 분")
    print(f"  납기 준수       : {due_perf['on_time_count']:,} / {due_perf['total_count']:,} 개")
    print(f"  납기 준수율     : {due_perf['due_date_adherence_rate']*100:.1f}%")
    print("=" * 60)

    _plot_utilization(utilization, output_dir)

    wip_times = [item[0] for item in collector.wip_log]
    wip_values = [item[1] for item in collector.wip_log]

    plt.figure(figsize=(11, 5))
    plt.step(wip_times, wip_values, where="post", color="#58a6ff", linewidth=2.0)
    plt.title("WIP 시계열")
    plt.xlabel("시간(분)")
    plt.ylabel("WIP")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "wip_timeseries.png"), dpi=160)
    plt.close()

    process_names = [
        name
        for name in config.get("common_routing", [])
        if name in real_stats and name in summary["stage_stats"]
    ]
    sim_process_times = [summary["stage_stats"][name]["avg_process_time"] for name in process_names]
    real_process_times = [real_stats[name]["mean_minutes"] for name in process_names]
    x = np.arange(len(process_names))
    width = 0.38

    plt.figure(figsize=(12, 5))
    plt.bar(x - width / 2, real_process_times, width=width, label="실측", color="#8b949e")
    plt.bar(
        x + width / 2,
        sim_process_times,
        width=width,
        label="시뮬",
        color=[process_lookup[name]["color"] for name in process_names],
    )
    plt.xticks(x, process_names, rotation=25, ha="right")
    plt.ylabel("분")
    plt.title("공정별 평균 처리시간 비교")
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "process_time_compare.png"), dpi=160)
    plt.close()

    sim_defect_rates = [summary["stage_stats"][name]["defect_rate"] * 100.0 for name in process_names]
    real_defect_rates = [real_stats[name]["defect_rate"] * 100.0 for name in process_names]

    plt.figure(figsize=(12, 5))
    plt.bar(x - width / 2, real_defect_rates, width=width, label="실측", color="#6e7681")
    plt.bar(
        x + width / 2,
        sim_defect_rates,
        width=width,
        label="시뮬",
        color=[process_lookup[name]["color"] for name in process_names],
    )
    plt.xticks(x, process_names, rotation=25, ha="right")
    plt.ylabel("불량률(%)")
    plt.title("공정별 불량률 비교")
    plt.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "defect_rate_compare.png"), dpi=160)
    plt.close()

    cycle_times = [
        item["cycle_time"]
        for item in collector.product_events
        if item.get("event_type") == "product_complete"
    ]
    plt.figure(figsize=(10, 5))
    if cycle_times:
        plt.hist(cycle_times, bins=24, color="#3fb950", edgecolor="black", alpha=0.8)
        plt.axvline(np.mean(cycle_times), color="#111111", linestyle="--", linewidth=1.4)
    plt.title("사이클 타임 분포")
    plt.xlabel("분")
    plt.ylabel("빈도")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "cycle_time_histogram.png"), dpi=160)
    plt.close()

    throughput_values = [summary["stage_stats"][name]["completed"] for name in process_names]
    plt.figure(figsize=(12, 5))
    plt.bar(
        process_names,
        throughput_values,
        color=[process_lookup[name]["color"] for name in process_names],
    )
    plt.xticks(rotation=25, ha="right")
    plt.ylabel("처리 건수")
    plt.title("공정별 처리량")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, "stage_throughput.png"), dpi=160)
    plt.close()

    if getattr(collector, "checkpoint_log", None):
        checkpoint_log = [
            item
            for item in collector.checkpoint_log
            if item.get("agent_scores") and item.get("agent_controls")
        ]

        if checkpoint_log:
            times = [item["sim_time"] for item in checkpoint_log]
            process_names = sorted(checkpoint_log[0]["agent_scores"].keys())

            plt.figure(figsize=(12, 6))
            for name in process_names:
                scores = [item["agent_scores"].get(name, 0.0) for item in checkpoint_log]
                plt.plot(times, scores, linewidth=1.8, label=name)
            plt.title("에이전트 점수 추세")
            plt.xlabel("시간(분)")
            plt.ylabel("점수")
            plt.legend(fontsize=8, ncol=2)
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, "agent_score_trend.png"), dpi=160)
            plt.close()

            fig, axes = plt.subplots(len(process_names), 1, figsize=(12, 3 * len(process_names)), sharex=True)
            if len(process_names) == 1:
                axes = [axes]
            for axis, name in zip(axes, process_names):
                control_names = sorted(checkpoint_log[0]["agent_controls"].get(name, {}).keys())
                for control_name in control_names:
                    values = [
                        item["agent_controls"].get(name, {}).get(control_name, 0.0)
                        for item in checkpoint_log
                    ]
                    axis.plot(times, values, linewidth=1.5, label=control_name)
                axis.set_title(f"{name} 제어값 추세")
                axis.set_ylabel("값")
                axis.legend(fontsize=8, ncol=2)
            axes[-1].set_xlabel("시간(분)")
            plt.tight_layout()
            plt.savefig(os.path.join(output_dir, "agent_control_trend.png"), dpi=160)
            plt.close(fig)

    return summary


def _plot_utilization(utilization, output_dir):
    """공정별 설비 가동률 / 가용률 bar chart를 저장한다."""
    if not utilization:
        return

    names = list(utilization.keys())
    util_rates = [utilization[n]["utilization_rate"] * 100 for n in names]
    avail_rates = [utilization[n]["availability_rate"] * 100 for n in names]

    x = np.arange(len(names))
    width = 0.38

    fig, ax = plt.subplots(figsize=(max(8, len(names) * 1.5), 5))
    bars1 = ax.bar(x - width / 2, util_rates, width=width, label="가동률", color="#58a6ff")
    bars2 = ax.bar(x + width / 2, avail_rates, width=width, label="가용률", color="#3fb950")

    for bar in bars1:
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                f"{bar.get_height():.1f}%", ha="center", va="bottom", fontsize=9)
    for bar in bars2:
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                f"{bar.get_height():.1f}%", ha="center", va="bottom", fontsize=9)

    ax.set_xticks(x)
    ax.set_xticklabels(names, rotation=15, ha="right")
    ax.set_ylabel("비율 (%)")
    ax.set_ylim(0, 115)
    ax.set_title("공정별 설비 가동률 / 가용률")
    ax.legend()
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()

    out_path = os.path.join(output_dir, "utilization_chart.png")
    plt.savefig(out_path, dpi=160, bbox_inches="tight")
    plt.close()
    print(f"📊 가동률 차트 저장됨: {out_path}")
