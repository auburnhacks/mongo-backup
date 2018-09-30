FROM python:3.6 as build
COPY requirements.txt .
RUN pip install --no-cache-dir --install-option="--prefix=/install" -r requirements.txt

FROM python:3.6-alpine
WORKDIR /app
COPY --from=build /install /usr/local
COPY backup.py .
ENTRYPOINT [ "python", "backup.py" ]