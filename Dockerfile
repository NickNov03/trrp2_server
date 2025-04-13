FROM python:3.10-slim
WORKDIR /app
COPY . .

RUN apt-get update && \
    apt-get install -y curl gnupg && \
    curl -fsSL https://deb.nodesource.com/setup_18.x | bash - && \
    apt-get install -y nodejs && \
    apt-get clean

COPY requirements.txt .

RUN npm install

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "server.py"]
