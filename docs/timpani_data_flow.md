
### Simplified Timpani Data flow

Using a 3rd party service like Junkipedia, users can provide a list off accounts on various social media services to monitor with regular scraping. This content can be collected via API.

When triggered from a schedule (or manually) the Booker queries a data source for new items and archives them in the Raw Store. The Conductor imports content items, and sends them out to other services to be processed. The content, annotations, and clusters are stored in the Content Store.  The Trend Viewer tool queries the Content Store to show trends and explore the items grouped in a cluster.

```mermaid
graph 
    Community_Posted_Content ---> Twitter_API
    Community_Posted_Content --> Telegram_API
    Community_Posted_Content --> Other_Socialmedia_APIs
    Lists_of_accounts --> Twitter_API
    Lists_of_accounts --> Telegram_API
    Twitter_API --hourly scraping--> Junkipedia_API
    Telegram_API --hourly scraping--> Junkipedia_API
    Junkipedia_API --daily queries--> Booker
    Other_Socialmedia_APIs--daily queries--> Booker
    Research_CSV_Archive --import once--> Booker
    Check_Shared_feed --daily queries--> Booker
    Booker -- archive chunks -->Raw_Store
    Raw_Store -- import content --> Conductor
    Content_Store -- process workflow --> Conductor -- update state --> Content_Store
    Conductor -- store embedding  --> ML_Vector_Store
    Conductor <--> Similarity_Clustering -- similar items --> Conductor 
    Similarity_Clustering --> ML_Vector_Store
    Conductor <--> Other_Services 
    Conductor <--> Keword_Extraction 
    Content_Store -- clusters and content items --> Trend_Viewer -- queries --> Content_Store
    Trend_Viewer --interesting trending items--> Check_Workspace
    Check_Workspace --> FactChecks_and_Explainers
    
```
