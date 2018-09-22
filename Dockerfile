FROM python:3.7-alpine
WORKDIR /usr/src/app

RUN apk --update add --virtual build-dependencies libffi-dev openssl-dev python-dev py-pip build-base
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
RUN python -m unittest discover unittests *_test.py

RUN cp -R . /usr/src/dist/
RUN rm -rf /usr/src/app
RUN cp -R /usr/src/dist/. /usr/src/app/
RUN rm -rf /usr/src/dist

COPY entrypoint.sh .
RUN chmod 755 entrypoint.sh
ENTRYPOINT ["/usr/src/app/entrypoint.sh"]
