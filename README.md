# Research and Development Project

![Code Coverage](https://img.shields.io/badge/Coverage-61%25-yellow.svg)

Calculating national and regional research and development expenditure as part of [national accounts](https://www.ons.gov.uk/economy/nationalaccounts).

This repository is currently being refounded towards a lean Dagster + Iceberg analytical pipeline. The local development path uses synthetic fixtures only and does **not** require production data, secrets, cloud credentials, or production object storage.

## What you can run locally

The most useful local path today is the refoundation Dagster synthetic demo:

- it runs the lean `src.randd_pipeline` assets against tiny synthetic CSV fixtures;
- it writes managed tables to a local SQLite-backed PyIceberg warehouse;
- it is suitable for install checks, smoke tests, and exploring the Dagster UI;
- it is not a production deployment.

The committed synthetic run configuration is `config/dagster/full_synthetic_v1_run_config.yaml`.

## Prerequisites

Install these tools before setting up the project:

- Python 3 with `venv` support. Python 3.10 or 3.11 is recommended for the smoothest dependency installation.
- Git.
- `make` for the documented macOS/Linux convenience commands. Windows users can either use Git Bash/WSL with `make`, or use the PowerShell commands below.

Check your Python and Git versions:

```bash
python --version
git --version
```

On Windows, if `python` is not recognised, try:

```powershell
py --version
git --version
```

## Install on Windows

These commands assume PowerShell from the repository root.

1. Clone the repository and enter it.

   ```powershell
   git clone <repository-url>
   cd research-and-development-dagster-iceberg
   ```

2. Create and activate a virtual environment.

   ```powershell
   py -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```

   If script execution is blocked, run PowerShell as your normal user and temporarily allow scripts for the current session:

   ```powershell
   Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
   .\.venv\Scripts\Activate.ps1
   ```

3. Upgrade packaging tools and install dependencies.

   ```powershell
   python -m pip install -U pip setuptools
   python -m pip install -r requirements-dev.txt
   pre-commit install
   ```

4. Confirm the local synthetic pipeline can run.

   ```powershell
   pytest -q tests/randd_pipeline/test_full_synthetic_v1_chain.py
   ```

## Install on macOS

These commands assume Terminal from the repository root.

1. Clone the repository and enter it.

   ```bash
   git clone <repository-url>
   cd research-and-development-dagster-iceberg
   ```

2. Create and activate a virtual environment.

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. Install the combined development dependencies.

   ```bash
   make requirements-dev
   ```

   If you do not want to use `make`, run the equivalent commands:

   ```bash
   python -m pip install -U pip setuptools
   python -m pip install -r requirements-dev.txt
   pre-commit install
   ```

4. Confirm the local synthetic pipeline can run.

   ```bash
   pytest -q tests/randd_pipeline/test_full_synthetic_v1_chain.py
   ```

## Install on Linux

These commands assume a shell from the repository root.

1. Clone the repository and enter it.

   ```bash
   git clone <repository-url>
   cd research-and-development-dagster-iceberg
   ```

2. Create and activate a virtual environment.

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. Install the combined development dependencies.

   ```bash
   make requirements-dev
   ```

   If you do not want to use `make`, run the equivalent commands:

   ```bash
   python -m pip install -U pip setuptools
   python -m pip install -r requirements-dev.txt
   pre-commit install
   ```

4. Confirm the local synthetic pipeline can run.

   ```bash
   pytest -q tests/randd_pipeline/test_full_synthetic_v1_chain.py
   ```

## Run the local Dagster synthetic demo

Use this path after completing the install steps for your operating system.

### macOS and Linux

Start the Dagster UI with:

```bash
make dagster-refoundation-local-demo
```

The equivalent command without `make` is:

```bash
python -m dagster dev -m src.randd_pipeline.local_demo_definitions
```

### Windows PowerShell

Start the Dagster UI with:

```powershell
python -m dagster dev -m src.randd_pipeline.local_demo_definitions
```

### Launch a synthetic run in the UI

1. Open the Dagster URL printed in the terminal, usually `http://127.0.0.1:3000`.
2. Open the asset graph or Launchpad for the implicit asset job.
3. Paste or load the run config from `config/dagster/full_synthetic_v1_run_config.yaml`.
4. Materialise the full synthetic chain.
5. Inspect materialisation events and asset checks in the run view.

The local demo definitions module is `src.randd_pipeline.local_demo_definitions`. The production-facing lean definitions module is `src.randd_pipeline.definitions`, which can be started with:

```bash
python -m dagster dev -m src.randd_pipeline.definitions
```

## Re-run safely

The local table store is deliberately non-mutating by default. Re-running the full synthetic chain against an existing local warehouse may fail if target tables already exist.

Before a repeated full run, either delete the default local demo warehouse:

```bash
rm -rf .tmp/refoundation-ui-warehouse
```

On Windows PowerShell, use:

```powershell
Remove-Item -Recurse -Force .tmp\refoundation-ui-warehouse
```

Or start Dagster with a fresh warehouse path.

macOS/Linux:

```bash
RND_PIPELINE_LOCAL_WAREHOUSE=.tmp/refoundation-ui-warehouse-run-2 python -m dagster dev -m src.randd_pipeline.local_demo_definitions
```

Windows PowerShell:

```powershell
$env:RND_PIPELINE_LOCAL_WAREHOUSE = ".tmp/refoundation-ui-warehouse-run-2"
python -m dagster dev -m src.randd_pipeline.local_demo_definitions
```

## Useful commands

| Task | macOS/Linux | Windows PowerShell |
| --- | --- | --- |
| Install development dependencies | `make requirements-dev` | `python -m pip install -U pip setuptools; python -m pip install -r requirements-dev.txt; pre-commit install` |
| Run refoundation package tests | `make test-randd-pipeline` | `pytest tests/randd_pipeline` |
| Run the synthetic end-to-end smoke test | `pytest -q tests/randd_pipeline/test_full_synthetic_v1_chain.py` | `pytest -q tests/randd_pipeline/test_full_synthetic_v1_chain.py` |
| Start local synthetic Dagster demo | `make dagster-refoundation-local-demo` | `python -m dagster dev -m src.randd_pipeline.local_demo_definitions` |
| Start production-facing lean definitions | `make dagster-refoundation-dev` | `python -m dagster dev -m src.randd_pipeline.definitions` |
| Build Sphinx docs | `make docs` | `sphinx-build -b html ./docs ./docs/_build` |

## Troubleshooting

### `python` is not found

- On macOS/Linux, try `python3` instead of `python` when creating the virtual environment.
- On Windows, try the Python launcher command `py`.

### `make` is not found on Windows

Use the PowerShell commands in this README instead of the `make` shortcuts, or run the macOS/Linux commands from WSL or Git Bash if your environment has `make` installed.

### PowerShell will not activate `.venv`

Temporarily allow script execution for the current PowerShell session:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

### Dagster run fails because tables already exist

Delete `.tmp/refoundation-ui-warehouse`, or set `RND_PIPELINE_LOCAL_WAREHOUSE` to a new path before starting Dagster.

### Dependency installation fails

Upgrade packaging tools inside the activated virtual environment and retry:

```bash
python -m pip install -U pip setuptools wheel
python -m pip install -r requirements-dev.txt
```

If the failure is platform-specific, include your operating system, Python version, and the failing command when asking for help.

## More documentation

- Full Dagster install, run, re-run, and troubleshooting notes: [Install and run the Dagster pipelines](./docs/user_guide/install_and_run_dagster.md).
- Refoundation developer setup notes: [Refoundation developer setup](./docs/developer/refoundation-dev-setup.md).
- Repository documentation index: [Documentation](./docs/README.md).

## Licence

Unless stated otherwise, the codebase is released under the MIT License. This covers
both the codebase and any sample code in the documentation. The documentation is ©
Crown copyright and available under the terms of the Open Government 3.0 licence.

## Acknowledgements

[This project structure is based on the `govcookiecutter` template
project][govcookiecutter]. Guidance on using the govcookiecutter can be found on [this youtube video](https://www.youtube.com/watch?v=N7_d3k3uQ_M) and in the [documentation here](https://dataingovernment.blog.gov.uk/2021/07/20/govcookiecutter-a-template-for-data-science-projects/).

Some of the text, especially that covering git configuration and security considerations was adapted from work by David Foster and Rowan Hemsi at ONS.

[contributing]: ./docs/contributor_guide/CONTRIBUTING.md
[govcookiecutter]: https://github.com/best-practice-and-impact/govcookiecutter
[docs-loading-environment-variables]: ./docs/user_guide/loading_environment_variables.md
[docs-loading-environment-variables-secrets]: ./docs/user_guide/loading_environment_variables.md#storing-secrets-and-credentials
