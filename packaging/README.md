# Isolated runtime artifacts

No package build command here deploys resources or invokes AWS. `tests/isolated/build/` is ignored by Git. The code archives are reproducible and include `tickit_control/`, never local AWS config, tests or credentials.

Chosen target: Lambda Python 3.11 x86_64 and Glue 5.0 (Spark 3.5.4, Python 3.11). Users/event execute their existing pandas code on Glue's Spark driver for this small acceptance test; listing/sales use the actual Spark candidate. All four jobs use two G.1X workers, five-minute timeout, no automatic retries/bookmarks/queuing. This is an isolated test configuration, not a proposed production sizing change.

```sh
python3 tools/prepare_aws_integration.py
python3.11 -m pip download --no-cache-dir --only-binary=:all: --platform manylinux2014_x86_64 --python-version 311 --implementation cp --abi cp311 --dest tests/isolated/build/wheels -r packaging/data.requirements.txt -r packaging/sdk.requirements.txt
python3.11 -m pip install --no-index --find-links tests/isolated/build/wheels --only-binary=:all: --platform manylinux2014_x86_64 --python-version 311 --implementation cp --abi cp311 --target tests/isolated/build/data-layer/python -r packaging/data.requirements.txt
python3.11 -m pip install --no-index --find-links tests/isolated/build/wheels --only-binary=:all: --platform manylinux2014_x86_64 --python-version 311 --implementation cp --abi cp311 --target tests/isolated/build/sdk-layer/python -r packaging/sdk.requirements.txt
python3 tools/build_isolated_dependencies.py
python3 tools/prepare_aws_integration.py
```

These commands were completed locally. The installer reports unrelated host-package conflicts because it can see the host environment, but installs into the separate target directories; it does not replace host packages. The downloaded wheel closure resolved successfully. The deployable layers exclude wheel tests, C/C++ headers, bytecode and command-line scripts. Runtime native `.so` files must have ELF headers. Lambda layers total **236,698,225 bytes (~225.7 MiB)**, plus under 1 MiB source, below the combined 250 MiB limit. Data layer ZIP is ~63 MiB, so upload via the isolated S3 artifact bucket rather than a direct ZIP API payload. No Linux binaries are imported on macOS.

Artifacts: five function source ZIPs, `tickit-control.zip`, four Glue scripts, SDK/data layer ZIPs, one `.gluewheels.zip`, four JSON templates and SHA-256 inventories. Wheels and deployment archives remain local, ignored files. `wheel-lock.json` captures downloaded bytes; `SHA256SUMS.json` captures deployable artifacts. Source templates under `infra/isolated/` and `stepfunctions/` are review copies of the builder output.

Glue uses `--extra-py-files` for source code, `--additional-python-modules` for the wheels ZIP and `--python-modules-installer-option=--no-index`. All dependencies and their transitives are pinned; native execution does not resolve packages from PyPI. SDK pinning supplies the tested S3 conditional-write API shape. Glue arguments include the full JSON `--BATCH_REQUEST`, isolated bucket, admission table and pipeline identity. `job_request` forwards only the two coordination settings into process environment.

Spark settings: UTC session timezone, TIMESTAMP_MICROS Parquet output, ANSI enabled, two shuffle partitions. CSV reads use explicit schema, ordered header validation and FAILFAST. Publication pins scratch versions and conditionally writes final parts; scratch is never a COPY source. Inputs/parts above the 5 GiB single-PUT transport limit fail explicitly.

PASS: target wheel availability/closure, ELF format, size, deterministic source ZIPs, entrypoint dispatch and argument wiring. PENDING: actual Linux import/Glue bootstrap, Spark/S3 committer behavior and native Parquet COPY. Downloading compatible wheels does not prove these runtime behaviors.

References: [Glue runtimes](https://docs.aws.amazon.com/glue/latest/dg/release-notes.html), [Glue library packaging](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-libraries.html).
