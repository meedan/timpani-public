FROM python:3.11-slim
# TODO: is there are docker image that already has the flask requirements?

# install dependencies
RUN apt-get update -qq && apt-get install --no-install-recommends -y \
    libpq-dev python3-dev curl

WORKDIR /usr/src/app

# copy src code
COPY . .

RUN pip install --upgrade pip

RUN pip install -r services/conductor/requirements.txt

# install timpani src as a python package
RUN pip install --editable .
HEALTHCHECK CMD curl --fail http://conductor:3101/healthcheck
ENTRYPOINT ["./services/conductor/docker-entrypoint.sh"]
CMD ["webapp"]  # default args
