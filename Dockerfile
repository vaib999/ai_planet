FROM python:3.10.5

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "linear_flow.py"]
