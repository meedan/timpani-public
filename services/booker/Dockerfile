FROM python:3.11-slim
MAINTAINER Meedan <sysops@meedan.com>


# install dependencies
RUN apt-get update -qq && apt-get install --no-install-recommends -y \
      libpq-dev 

# TODO: symlink python to python3      

WORKDIR /usr/src/app

# copy all the src files
COPY . .

RUN pip install --upgrade pip


RUN pip install -r services/booker/requirements.txt

# install timpani src as a python package
RUN pip install --editable .


# TODO: I'd like to be able to pass in env as argument 
# and also be able to specify test and a couple of other specific functions
# that can be called without leaving a service running
# https://aws.amazon.com/blogs/opensource/demystifying-entrypoint-cmd-docker/

ENTRYPOINT ["./services/booker/docker-entrypoint.sh"]
CMD ["stayup"]  # default args
