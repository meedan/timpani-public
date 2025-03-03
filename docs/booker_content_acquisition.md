
### Booker content aquistion

Sequence diagram of calls for the 'Booker' content acquisition orchestrator to the Junkipedia 3rd party API (for example). 
When triggered from a schedule (or manually) the Booker processes loads up workspace configurations to find out what queries to run, and then pages the API and appends the content chunks into appropriate partitions in the Raw Store. 

```mermaid
sequenceDiagram
    participant Junkipedia API
    participant Booker
    participant Raw Store

    
    Booker->>Booker: schedualed acquistion trigger
    activate Booker
    Booker->>Booker: load Workspace_1 configs
    Booker->>+Junkipedia API: any content for query_1?
    Junkipedia API-->>-Booker: page 1 of query_1
    Booker->>Raw Store: store page 1 as ChunkA

    Booker->>+Junkipedia API: next page of query_1?
    Junkipedia API-->>-Booker: page 2 of query_1
    Booker->>Raw Store: store page 2 as ChunkB
    deactivate Booker
```
