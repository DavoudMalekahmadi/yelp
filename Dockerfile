FROM python:3.9.12
WORKDIR /code
ADD ./requirements.txt /code/requirements.txt
RUN pip install --upgrade setuptools wheel
RUN pip install -r requirements.txt
COPY . /code
