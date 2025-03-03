FROM python:3.11-slim
# TODO: is there are docker image that already has the flask requirements?

# install dependencies
RUN apt-get update -qq && apt-get install --no-install-recommends -y \
    libpq-dev python3-dev \
    build-essential \
    curl \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

RUN pip install --upgrade pip

# copy just requirements so cache for the rest of the build is not invalidated
COPY ./services/trend_viewer/requirements.txt ./services/trend_viewer/requirements.txt
RUN pip install -r services/trend_viewer/requirements.txt

# copy src code
COPY . .

# install timpani src as a python package
RUN pip install --editable .
EXPOSE 8501
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health
ENTRYPOINT ["./services/trend_viewer/docker-entrypoint.sh"]
CMD ["viewer"]  # default args
