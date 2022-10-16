FROM python:3.10

RUN mkdir -p /code
WORKDIR /code

COPY . .

RUN pip install --upgrade pip
RUN pip install -r requirements.txt
CMD [ "python", "main.py" ]
