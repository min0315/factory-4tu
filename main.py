# main.py
# 4TU 실측 데이터 기반 시뮬레이션을 실행하고 PNG/HTML 결과물을 생성한다.

import argparse
import copy
import json
import os

from agent_manager import AgentManager
from config import FACTORY_CONFIG, REAL_DATA_DF
from export_csv import export_results
from report import generate_report
from simulation import run_simulation


def _build_runtime_config(args):
    """CLI 인자를 반영한 실행 설정을 생성한다."""
    config = copy.deepcopy(FACTORY_CONFIG)
    config["sim"]["duration"] = int(args.duration)
    config["sim"]["random_seed"] = int(args.seed)
    return config


def _generate_3d_html(output_path, config, collector):
    """Three.js 기반 3D 시뮬레이션 대시보드 HTML을 생성한다."""
    summary = collector.get_summary()
    compare = collector.compare_with_real(REAL_DATA_DF)
    process_lookup = {item["name"]: item for item in config["processes"]}
    station_order = [
        "Turning_Milling",
        "QC_Turning",
        "Lapping",
        "Round_Grinding",
        "Flat_Grinding",
        "Laser_Marking",
        "Final_Inspection",
        "Packing",
    ]
    stations = []
    for name in station_order:
        if name not in process_lookup:
            continue
        stage_summary = summary["stage_stats"].get(name, {})
        real_stage = config["data_summary"]["real_process_stats"].get(name, {})
        stations.append(
            {
                "name": name,
                "color": process_lookup[name]["color"],
                "machines": process_lookup[name]["num_machines"],
                "simAvg": stage_summary.get("avg_process_time", 0.0),
                "realAvg": real_stage.get("mean_minutes", 0.0),
                "simDefect": round(stage_summary.get("defect_rate", 0.0) * 100.0, 2),
                "realDefect": round(real_stage.get("defect_rate", 0.0) * 100.0, 2),
                "throughput": stage_summary.get("completed", 0),
            }
        )

    payload = json.dumps(stations, ensure_ascii=False)
    common_routing = " → ".join(config.get("common_routing", []))

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>4TU Production Simulation</title>
  <style>
    :root {{
      --bg-1: #08131f;
      --bg-2: #0f2233;
      --panel: rgba(6, 16, 24, 0.82);
      --line: rgba(255,255,255,0.14);
      --text: #edf6ff;
      --muted: #9cb4c7;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      overflow: hidden;
      color: var(--text);
      font-family: AppleGothic, "Malgun Gothic", sans-serif;
      background:
        radial-gradient(circle at 12% 18%, rgba(88,166,255,0.18), transparent 26%),
        radial-gradient(circle at 85% 12%, rgba(255,123,114,0.14), transparent 24%),
        linear-gradient(145deg, var(--bg-1), var(--bg-2));
    }}
    #scene {{ width: 100vw; height: 100vh; }}
    .panel {{
      position: fixed;
      top: 18px;
      right: 18px;
      width: 360px;
      padding: 18px;
      border: 1px solid var(--line);
      border-radius: 18px;
      background: var(--panel);
      backdrop-filter: blur(14px);
      box-shadow: 0 24px 60px rgba(0, 0, 0, 0.32);
    }}
    .panel h1 {{
      margin: 0 0 8px;
      font-size: 22px;
    }}
    .sub {{
      margin: 0 0 12px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.45;
    }}
    .kpi {{
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 6px 10px;
      margin: 10px 0;
      padding-bottom: 10px;
      border-bottom: 1px solid var(--line);
      font-size: 14px;
    }}
    .kpi span:first-child {{ color: var(--muted); }}
    .legend {{
      position: fixed;
      left: 18px;
      bottom: 18px;
      width: 420px;
      max-width: calc(100vw - 36px);
      padding: 16px 18px;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: rgba(8, 19, 31, 0.78);
      backdrop-filter: blur(14px);
    }}
    .legend h2 {{
      margin: 0 0 10px;
      font-size: 15px;
    }}
    .legend-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 8px 12px;
    }}
    .legend-item {{
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 13px;
      color: var(--text);
    }}
    .dot {{
      width: 11px;
      height: 11px;
      border-radius: 50%;
      flex: 0 0 auto;
    }}
    @media (max-width: 900px) {{
      .panel {{
        top: 12px;
        left: 12px;
        right: 12px;
        width: auto;
      }}
      .legend {{
        left: 12px;
        right: 12px;
        bottom: 12px;
        width: auto;
      }}
      .legend-grid {{
        grid-template-columns: 1fr;
      }}
    }}
  </style>
</head>
<body>
  <div id="scene"></div>
  <aside class="panel">
    <h1>4TU Production Simulation</h1>
    <p class="sub">대표 라우팅: {common_routing}</p>
    <div class="kpi"><span>실측 케이스 수</span><strong>{compare["real_case_count"]}</strong></div>
    <div class="kpi"><span>시뮬 시작 케이스 수</span><strong>{compare["sim_started_cases"]}</strong></div>
    <div class="kpi"><span>시뮬 완료 케이스 수</span><strong>{compare["sim_completed_cases"]}</strong></div>
    <div class="kpi"><span>실측 평균 도착간격</span><strong>{compare["real_avg_inter_arrival_minutes"]:.1f}분</strong></div>
    <div class="kpi"><span>시뮬 평균 사이클 타임</span><strong>{summary["avg_cycle_time"]:.1f}분</strong></div>
    <div class="kpi"><span>시뮬 평균 WIP</span><strong>{summary["avg_wip"]:.1f}</strong></div>
    <div class="kpi"><span>시뮬 총 불량률</span><strong>{summary["defect_rate"] * 100:.2f}%</strong></div>
  </aside>
  <section class="legend">
    <h2>공정 스테이션</h2>
    <div class="legend-grid" id="legend-grid"></div>
  </section>

  <script src="https://cdn.jsdelivr.net/npm/three@0.160.0/build/three.min.js"></script>
  <script>
    const stations = {payload};
    const scene = new THREE.Scene();
    const camera = new THREE.PerspectiveCamera(52, window.innerWidth / window.innerHeight, 0.1, 1000);
    const renderer = new THREE.WebGLRenderer({{ antialias: true, alpha: true }});
    renderer.setPixelRatio(window.devicePixelRatio || 1);
    renderer.setSize(window.innerWidth, window.innerHeight);
    document.getElementById("scene").appendChild(renderer.domElement);

    camera.position.set(0, 18, 38);
    camera.lookAt(0, 0, 0);

    scene.add(new THREE.AmbientLight(0xffffff, 1.2));
    const light = new THREE.DirectionalLight(0xffffff, 1.3);
    light.position.set(8, 18, 10);
    scene.add(light);

    const floor = new THREE.Mesh(
      new THREE.PlaneGeometry(90, 34),
      new THREE.MeshPhongMaterial({{ color: 0x112132, shininess: 28 }})
    );
    floor.rotation.x = -Math.PI / 2;
    floor.position.y = -2.4;
    scene.add(floor);

    const lane = new THREE.Mesh(
      new THREE.PlaneGeometry(82, 6),
      new THREE.MeshBasicMaterial({{ color: 0x1d3a53, transparent: true, opacity: 0.55 }})
    );
    lane.rotation.x = -Math.PI / 2;
    lane.position.set(0, -2.35, 0);
    scene.add(lane);

    const meshes = [];
    stations.forEach((station, index) => {{
      const x = -31 + index * 9;
      const height = 2.8 + (station.machines * 0.8);
      const base = new THREE.Mesh(
        new THREE.CylinderGeometry(3.2, 3.6, 0.9, 32),
        new THREE.MeshPhongMaterial({{ color: 0x203446 }})
      );
      base.position.set(x, -1.95, 0);
      scene.add(base);

      const tower = new THREE.Mesh(
        new THREE.BoxGeometry(4.8, height, 4.8),
        new THREE.MeshPhongMaterial({{
          color: station.color,
          emissive: station.color,
          emissiveIntensity: 0.18,
          shininess: 48
        }})
      );
      tower.position.set(x, height / 2 - 1.5, 0);
      scene.add(tower);
      meshes.push({{ mesh: tower, offset: index * 0.6 }});
    }});

    const productDots = [];
    for (let i = 0; i < 28; i += 1) {{
      const dot = new THREE.Mesh(
        new THREE.SphereGeometry(0.25, 18, 18),
        new THREE.MeshBasicMaterial({{ color: 0xf0f6fc }})
      );
      dot.position.set(-36 + i * 2.8, -1.2 + (i % 3) * 0.18, 0);
      scene.add(dot);
      productDots.push({{ mesh: dot, speed: 0.045 + (i % 5) * 0.008 }});
    }}

    const legendGrid = document.getElementById("legend-grid");
    legendGrid.innerHTML = stations.map((station) => `
      <div class="legend-item">
        <span class="dot" style="background:${{station.color}}"></span>
        <span>${{station.name}} | 실측 ${{station.realAvg.toFixed(1)}}분 / 시뮬 ${{station.simAvg.toFixed(1)}}분</span>
      </div>
    `).join("");

    function animate() {{
      requestAnimationFrame(animate);
      const t = performance.now() * 0.001;
      meshes.forEach(({{ mesh, offset }}) => {{
        mesh.rotation.y += 0.006;
        mesh.position.y = Math.sin(t + offset) * 0.22 + mesh.geometry.parameters.height / 2 - 1.5;
      }});
      productDots.forEach((item, idx) => {{
        item.mesh.position.x += item.speed;
        item.mesh.position.z = Math.sin(t * 2 + idx) * 1.1;
        if (item.mesh.position.x > 36) item.mesh.position.x = -36;
      }});
      scene.rotation.y = Math.sin(t * 0.22) * 0.05;
      renderer.render(scene, camera);
    }}

    animate();

    window.addEventListener("resize", () => {{
      camera.aspect = window.innerWidth / window.innerHeight;
      camera.updateProjectionMatrix();
      renderer.setSize(window.innerWidth, window.innerHeight);
    }});
  </script>
</body>
</html>
"""

    with open(output_path, "w", encoding="utf-8") as file_obj:
        file_obj.write(html)


def main():
    """시뮬레이션 실행과 리포트 생성을 순차적으로 수행한다."""
    parser = argparse.ArgumentParser(description="4TU Production Simulation")
    parser.add_argument("--duration", type=int, default=127000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--agents", action="store_true")
    parser.add_argument("--checkpoint", type=int, default=3000)
    args = parser.parse_args()

    config = _build_runtime_config(args)
    output_dir = os.path.dirname(__file__)
    manager = AgentManager(args.checkpoint) if args.agents else None
    if manager is not None:
        print(
            f"LLM agent mode 활성화: Ollama({args.checkpoint}분 체크포인트, qwen2.5:7b)"
        )
    collector = run_simulation(
        config,
        agent_manager=manager,
        checkpoint_minutes=args.checkpoint,
    )
    generate_report(collector, config, output_dir)
    export_results(collector, config, output_dir)
    _generate_3d_html(os.path.join(output_dir, "simulation_3d.html"), config, collector)
    if manager is not None:
        manager.print_policy_summary()


if __name__ == "__main__":
    main()
