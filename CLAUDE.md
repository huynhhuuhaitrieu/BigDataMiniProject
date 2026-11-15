# CLAUDE.md

The shared project context for every AI assistant is in `AGENTS.md`, imported below. Edit that file, not this one, so Claude and GPT-based tools keep the same information.

@AGENTS.md

## Claude Code specifics

- `.claude/launch.json` defines the `webui` preview server (`.venv-anomaly/bin/python webui/server.py`, port 8790). Use it to show the demo website in the preview pane.
- Run Spark and Kafka benchmarks (`assess/run_assessment.py live capacity recovery`) in the background. They take 20–30 minutes.
- When charts or `assess/results/*.png` change, view the PNG to check labels and overlaps before reporting.
