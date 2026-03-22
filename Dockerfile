# Multi-role image: master, mapper, reducer
# Role selected via ROLE env var or command override
FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY src/mapreduce/ .

# Default: run master
ENV ROLE=master
ENV PYTHONUNBUFFERED=1

CMD ["sh", "-c", "python ${ROLE}.py"]
