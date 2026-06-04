# Research and Development Project

![Code Coverage](https://img.shields.io/badge/Coverage-61%25-yellow.svg)

Calculating national and regional research and development expenditure as part of [national accounts](https://www.ons.gov.uk/economy/nationalaccounts).

Additional information about the aims and objectives of the project will go here when it is available. The project is currently in pre-discovery.

## Dagster pipelines quickstart

The current refoundation Dagster path can be run locally with synthetic fixtures only; it does not require production data, secrets, or cloud credentials.

1. Create and activate a virtual environment.

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   ```

2. Install the combined development dependencies.

   ```bash
   make requirements-dev
   ```

3. Run the local synthetic end-to-end smoke test.

   ```bash
   pytest -q tests/randd_pipeline/test_full_synthetic_v1_chain.py
   ```

4. Start the local Dagster UI demo.

   ```bash
   make dagster-refoundation-local-demo
   ```

5. Open the Dagster URL printed in the terminal, usually `http://127.0.0.1:3000`, and launch a run using `config/dagster/full_synthetic_v1_run_config.yaml`.

For full install, run, re-run, and troubleshooting notes, see [Install and run the Dagster pipelines](./docs/user_guide/install_and_run_dagster.md).

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
