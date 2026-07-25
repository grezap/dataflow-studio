# Media assets

| Asset | What | How it's produced |
|---|---|---|
| `dataflow-studio-demo.gif` | The hero walkthrough (build → tests → Aspire → Docker → the three planes) | Rendered from [`scripts/demo.tape`](../../scripts/demo.tape) by VHS — reproducibly in CI (`.github/workflows/demo-gif.yml`) or locally with `vhs scripts/demo.tape`. |
| `aspire-dashboard.png` | The .NET Aspire dashboard — the Api + the pipeline consoles as first-class resources | `dotnet run --project src/DataFlowStudio.AppHost`, then screenshot the dashboard. |
| `marquez-lineage.png` | The Marquez OpenLineage graph (`oltp.* → dfs.* → dwh.*`, 2 jobs / 29 datasets) | Rendered against the live lab Marquez (`https://192.168.70.127`) after `scripts/dfs-lineage-demo.ps1`. |
| `tempo-trace.png` | The Grafana/Tempo trace for a run (`curation.drain` + one `curate` per record) | Rendered against the live lab Grafana after `scripts/dfs-otel-demo.ps1`. |

The GIF and the Aspire dashboard render from this repo alone; the Grafana/Marquez shots render against the
NexusPlatform lab (the observability + platform-tools tiers). See [`../case-study.md`](../case-study.md).
