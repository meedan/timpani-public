import datetime
import json
import uuid


class Item(object):
    """
    An individual record that will be written into the Raw Store
    with associated metadata. This effectively defines a
    schema of availible fields
    """

    DATE_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

    # These fields should only be modified at object instantiation
    run_id = None
    workspace_id = None
    source_id = None
    query_id = None
    page_id = None
    created_at = None  # should be a date
    content_id = (
        uuid.uuid4().hex
    )  # unique id for the content, hopefully from source, but UUID is default
    content = None  # this will be a dict we can convert to json?
    # TODO: run_id

    def __init__(
        self,
        run_id: str,
        workspace_id: str,
        source_id: str,  # i.e. junkipedia
        query_id: str,  # maps back to search terms
        page_id: str,  # for mapping to chunk of multipage request
        content_id: str,
        content: dict,  # this will be a json blob some data
    ):
        """
        Class constructor
        @param content_id will default to a UUID if not provided
        """
        self.run_id = run_id
        self.workspace_id = workspace_id
        self.source_id = source_id
        self.query_id = query_id
        self.page_id = page_id
        self.created_at = datetime.datetime.utcnow()
        if content_id is not None:
            # use uuid if missing
            self.content_id = content_id
        self.content = content

    def toJSON(self):
        # TODO: option to truncate content if it is big for debugging
        # TODO: if content not a dict, need to treat as text and json escape
        obj = {
            "run_id": self.run_id,
            "workspace_id": self.workspace_id,
            "source_id": self.source_id,
            "query_id": self.query_id,
            "page_id": self.page_id,
            "created_at": self.created_at.strftime(self.DATE_FORMAT),
            "content_id": self.content_id,
            "content": self.content,
        }
        return json.dumps(obj)

    @classmethod
    def fromJSON(cls, json_string: str):
        """
        Constructor to return object from json
        """
        try:
            obj = json.loads(json_string)
            item = cls(
                run_id=obj["run_id"],
                workspace_id=obj["workspace_id"],
                source_id=obj["source_id"],
                query_id=obj["query_id"],
                page_id=obj["page_id"],
                content_id=obj["content_id"],
                content=obj["content"],
            )
        except json.decoder.JSONDecodeError as e:
            print(f"Error '{e}' while reloading raw item from {json_string}")
            raise e
        item.created_at = datetime.datetime.strptime(obj["created_at"], cls.DATE_FORMAT)
        return item
