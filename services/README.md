This directory contains the Dockerfile definitions to build images each of the services needed to run Timpani
* There should be corresponding github actions to build the image in `.github/workflows/`
* The services mostly use code from the corresponding module in the `/timpani/` python pakcage, but there is
some code that will be used by multiple servies 
* Tests for each service must be run from the appropriate image so that requirements are present

TODO: need to seperate ECR image tags when they are built.  And maybe better to have one action with seperate build steps for each image?
TODO: requirements.txt should go in the python package
TODO: Maybe this is where we should also store info for the various sub-services in different environments? i.e. minio vs aws s3, postgres image in container vs postgres cluster?
