### Conductor content processing and querying

Sequence diagram of calls for the 'Conductor' content processing orchestrator.  This shows:
* new content processing sequence -- content aquistion in the raw store triggers ETL to normalized objects which are stored in the Content store with appropriate vector indexes constructed
*  The old content removal sequence -- content older than two weeks removed from system with appropriate updates to linked objects
* processing an SQL query from a BI Tool -- query is directly to relational database layer for aggregation, and later drill down to individual records that user decides should be recorded in Check for annotation
* handinling a query from a bot in Check -- newly arriving content is vectorized and any matching items are returned to show to user as context for the fact checking process


```mermaid
sequenceDiagram
    participant Check
    participant BI Tool
    participant Raw Store
    participant Content Store
    participant Conductor
    
    box Model Services (Presto?)
    participant Language Detection
    participant Text Vectorization
    end
    box Alegre
    participant Indexing Store
    end
    
    Note right of Raw Store: new content processing
    Raw Store->>Conductor: New data "A" notification
    activate Conductor
    Conductor-->>+Raw Store: Fetch a chunk of "A"
    Raw Store->>-Conductor: Here is "A1" 
    Conductor->>+Content Store: create ItemA for row in A1
    Content Store-->>-Conductor: ItemA with id 'ItemA'
    Conductor->>Conductor: load content mappings

    Conductor->>+Language Detection: get language for ItemA's content
    Language Detection-->>-Conductor: Spanish
    Conductor->>Content Store: Save ItemA state
    Conductor->>+Text Vectorization: get vector for ItemA
    Text Vectorization-->>-Conductor: Vector A
    Conductor->>+Indexing Store: cache vector and get similiar items
    Indexing Store-->-Conductor: ItemB, ItemC
    Conductor->>Conductor: clustering logic
    Conductor->>Content Store: create cluster [ItemA, ItemB]
    Content Store-->>Conductor: Cluster1
    Conductor-)Content Store: Save ItemA state
    deactivate Conductor

    Note right of Conductor: Old content removal
    Conductor->>Conductor: schedule trigger
    activate Conductor
    Conductor->>+Content Store: any content older than 2 weeks?
    Content Store-->>-Conductor: ItemB
    Conductor->>+Indexing Store: delete Vector B
    Indexing Store-->>-Conductor: ok
    Conductor->>+Content Store: delete ItemB, update Cluster1
    Content Store-->>-Conductor: ok
    deactivate Conductor

    Note Right of BI Tool: Trending item query
    activate BI Tool
    BI Tool->>BI Tool: user query
    BI Tool->>+Content Store: SELECT CLUSTER HAVING MOST ITEMS
    Content Store-->>-BI Tool: Cluster1
    BI Tool->>BI Tool: user drilldown
    BI Tool->>+Content Store: SELECT ITEMS IN CLUSTER1
    Content Store-->>-BI Tool: ItemA
    BI Tool->>Check: store ItemA as new ProjectMedia
    deactivate BI Tool

    Note Right of Check: ProjectMedia Bot query
    activate Check
    Check->>Check: new ProjectMedia created
    Check->>+Conductor: Any similar items for text D?
    Conductor->>+Text Vectorization: get vector for text D
    Text Vectorization-->>-Conductor: Vector D
    Conductor->>+Indexing Store: any similar items for VectorD?
    Indexing Store-->>-Conductor: Vector A
    Conductor->>+Content Store: get ItemA
    Content Store->>-Conductor: ItemaA
    Conductor-->>-Check: ItemA is similar
    deactivate Check

```
