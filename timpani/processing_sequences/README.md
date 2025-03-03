# Workflow Processing sequences

Workflows define the processing sequence steps: what fields from the original content should be used, how they should be transformed, what models they should be sent to, etc.

Usually they include 
- a `ContentItemState` model that defines the allowable states the data can be in.  This can extend a simper model by added additinal states. 
- an `extract_items()` function that describes which fields (and which pieces of content) will be extracted from the raw content item and loaded into the content_store
- and implementation of the `next_state()` function that, based on the state the content item is in, calls the next appropriate model or transformation in the sequence. 


## Creating a new workflow for a worksspace

- Copy an existing workflow in the `/processing_sequences/` directory that is similar to a transformation process needed
- Rename the class and set the `get_name()` response to a unqiue id_value for the workflow. This value will go in the workflow_id of the workspace cfg file
- If the workflow needs a custom state model, give it a unique class name, and make sure that name also shows up in the value for `"polymorphic_identity:"`.  
- If this workflow will be adding new models or transformations, update the state model and state transformation mappings to account for them 
- Update `get_state_model()` to return an instance of the state model class
- Update `/processing_sequences/workflow_manager.py` to import the new workflow model and add it to `REGISTRED_WORKFLOWS` so the conductor will know where to find it
- Update the `extract_items()` function to pull the fields from the raw items
- Make sure that the `next_state()` model has a mapping for each of the states (or will pass some up to super class if it is extending)
