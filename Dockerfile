FROM tiangolo/uvicorn-gunicorn-fastapi:python3.9-slim
ENV MODULE_NAME="dijon.main"

# required for bsdiff4 to pip install
RUN apt-get update && apt-get install -y \
  gcc \
  && rm -rf /var/lib/apt/lists/*

# this default main.py doesn't do anything, but i don't like it being there
RUN rm /app/main.py

# get latest pip and poetry. poetry generates a requirements.txt, pip installs the requirements.txt
RUN pip install --upgrade pip
RUN pip install poetry==1.1.8

# this script runs automatically at start
COPY ./prestart.sh /app/

# generate requirements.txt and install it to the system python
COPY ./pyproject.toml ./poetry.lock /app/
RUN poetry export -o requirements.txt && pip install -r requirements.txt && rm -f requirements.txt

# add the app and install it to the system python
COPY ./dijon /app/dijon
RUN pip install .
