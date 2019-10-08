[![CircleCI](https://circleci.com/gh/mozilla/leanplum_data_export.svg?style=svg)](https://circleci.com/gh/mozilla/leanplum_data_export)

# Leanplum Data Export
This repository is the job to export Mozilla data from Leanplum and into BQ.

## Development and Testing

While iterating on development, we recommend using virtualenv
to run the tests locally.

### Run tests locally

Install requirements locally:
```
python3 -m virtualenv venv
source venv/bin/activate
make install-requirements
```

Run tests locally:
```
pytest tests/
```

### Run tests in docker

You can run the tests just as CI does by building the container
and running the tests.

```
make clean && make build
make test
```

### Deployment

This project deploys automatically to Dockerhub. The latest release is used to run the job.
