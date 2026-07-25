# DataFlow Studio — portfolio walkthrough, scripted for VHS (https://github.com/charmbracelet/vhs).
#
# Render:  vhs scripts/demo.tape        ->  docs/media/dataflow-studio-demo.gif
# Prereqs: the .NET 10 SDK, Docker, GITHUB_PACKAGES_TOKEN set (gh auth token), and the dfs-api image
#          pre-built (deploy/README.md) so `docker run` is instant on camera.
#
# The tape tells the v0.1.0 story: one modular-monolith solution, tested to the E12 gate, orchestrated
# by .NET Aspire, and packaged as containers — feeding a CDC → curated → DWH pipeline that observes
# itself across three planes (Tempo traces, ClickHouse pipeline_events, Marquez lineage).

Output docs/media/dataflow-studio-demo.gif

Set Shell "bash"
Set FontSize 17
Set Width 1280
Set Height 720
Set Theme "Catppuccin Mocha"
Set Padding 18
Set TypingSpeed 45ms

Hide
Type "clear" Enter
Show

# --- 1. The solution -----------------------------------------------------------------------------
Type "# DataFlow Studio — a modular monolith: OLTP -> CDC -> curated Avro -> StarRocks DWH + ClickHouse"
Enter
Sleep 1s
Type "dotnet build DataFlowStudio.slnx -c Release --nologo | tail -3"
Enter
Sleep 6s

# --- 2. Tested to the E12 gate -------------------------------------------------------------------
Type "# 113 tests. Logic coverage 93% line / 85% branch — every logic assembly >= 80/80 (ADR-0012)."
Enter
Sleep 1s
Type "dotnet test tests/DataFlowStudio.UnitTests -c Release --no-build | tail -1"
Enter
Sleep 5s

# --- 3. Orchestrated by .NET Aspire --------------------------------------------------------------
Type "# One command brings up the whole topology under the Aspire dashboard (ADR-0013)."
Enter
Sleep 1s
Type "# dotnet run --project src/DataFlowStudio.AppHost   ->   dfs-api + dfs-{seed,curation,warehouse-sink,...}"
Enter
Sleep 2s

# --- 4. Packaged as containers -------------------------------------------------------------------
Type "# Packaged non-root; the Api serves /health with all four modules wired."
Enter
Sleep 1s
Type "docker run -d --rm -p 8081:8080 --name dfs dfs-api:latest >/dev/null && sleep 3"
Enter
Sleep 4s
Type "curl -s localhost:8081/health && echo && curl -s localhost:8081/modules && echo"
Enter
Sleep 3s
Type 'docker stop dfs >/dev/null'
Enter
Sleep 1s

# --- 5. The three self-observation planes --------------------------------------------------------
Type "# Every pipeline run is one correlated entity across three planes:"
Enter
Type "#   OTel trace id  ==  ClickHouse pipeline_events.trace_id  ==  Marquez runId"
Enter
Sleep 3s
Type "# See docs/watch-the-pipeline.md for the full CDC -> curated -> DWH replay."
Enter
Sleep 3s
