# Lambda container image for api_bronze. One image serves every API source:
# each Lambda function differs only by its event/config, not by its code.
#
# Build from the repo root (the build context must contain src/ and docker/):
#   uv export --no-dev --no-emit-project --extra aws --format requirements-txt -o docker/requirements.lock
#   docker build -f docker/Dockerfile --platform linux/arm64 -t api-bronze .
# Use linux/amd64 for x86_64 functions. The function's architecture must match.
FROM public.ecr.aws/lambda/python:3.12

COPY docker/requirements.lock /tmp/requirements.lock
RUN pip install --no-cache-dir --require-hashes -r /tmp/requirements.lock --target "${LAMBDA_TASK_ROOT}" \
 && rm /tmp/requirements.lock

COPY src/api_bronze "${LAMBDA_TASK_ROOT}/api_bronze"

# Only /tmp is writable on Lambda. dlt would otherwise write under $HOME.
ENV DLT_DATA_DIR=/tmp/dlt \
    DLT_PROJECT_DIR=/tmp \
    RUNTIME__DLTHUB_TELEMETRY=false \
    PYTHONDONTWRITEBYTECODE=1

CMD ["api_bronze.handler.handler"]
