# Workspace configs
A 'workspace' defines a space of content that is eligible to be matched together.  It maps querys on content sources for a given time range to partitions in the 'raw_store' where blobs of raw content items can be acquired and cached for later processing. 
* it defines a unique id to be used everywhere for this workspace 
* it lists the content source(s) to be used by the booker service to acquire and archive content
* it lists sets of queries for the dasources (i.e. Junkipedia keyword "List" ids)
* it defines the id of the workflow that should will be used by the conductor to load and process the data
* it defines an authorization mapping to determine who will have access to the data in the Trend Viewer (if enabled)

# Setting up a new workspace

- Copy and rename an existing workspace `_cfg.py` file in the `/workspace_config/` directory, ideally one that uses a similar data source. For example, `meedan_cfg.py` to work with a junkipedia source. 
- Rename the class and set a unique workspace id in `get_workspace_slug()` 
- Import new config class into `workspace_config_manager.py` and add it to the list of active configurations in `REGISTRED_WORKSPACE_CONFIGURATIONS` so timpani knows it exists
- update the mapping of content sources to queries in `get_queries`. i.e. for the junkipedia datasource, return a list of junkipedia queries
- add any access tokens or API keys needed to access the content_source
    - for Junkipedia, this means creating an encrypted entry for the api in the AWS Systems Manager Parameter store ("SSM") 
- define a workflow id in `get_workflow_id` 
    - see /processing_sequences/README for setting up a new workflow if needed
    - if a new workflow is added, update the test in `conductor/test/test_workflows.py` to cover it
- When enabled in Meedan systems `get_authed_check_workspace_slugs()` needs to a list of authorized Check workspaces. Anyone with access to those Check workspaces will have access to this Timpani workspace in the Trend Viewer (using Check's session cookie).  In addition, returning values of `'public'` will allow anyone to access, `'dev_public'` will allow access during local development. See `/timpani/util/meedan_auth.py` for a description of permissions structure and possible values. 

# Testing

To run all of the unit- and integration-tests for each service, the apropriate container can be started in test mode with the directory of the tests.

First, make sure the appropriate services are running with `docker compose up` and then:

```
docker compose run conductor test timpani/conductor/test    
```
Note that the tests involving clustering will need the Alegre system running and accessible

```
docker compose run booker test timpani/booker/test    
```
Note that the booker acquire script (and many of the booker integration tests) will need to login to AWS to download and decrypt service access tokens from AWS SSM, as mentioned above. 


```
docker compose run trend_viewer test timpani/trend_viewer/test    
```
The trend_viewer app should be visible in the browser on `http://localhost:8501/` 


To try out data acquisition for the new workspace, run:
```
docker compose run booker acquire --workspace_id=<workspace_name> --trigger_ingest=True
```

adding --triger_ingest=True will trigger the workflow, so it should process the data all the way through to be visible in the local trend_viewer. 



To test a new workflow alone, confirm that basic workflow initialization works (by processing a single item) by running:
```
docker compose run conductor test timpani/conductor/test test_workflows.py    
```

The workflows that involve clustering will need that the Alegre meedan similarity service is also running and accessible
