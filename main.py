from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from azure.cosmos import CosmosClient

app = FastAPI(title="FastAPI + CosmosDB (super simple)")

# ----------------------------
# Cosmos DB config (edit these 3 lines)
# ----------------------------
COSMOS_ENDPOINT = "https://fastcosmosdb.documents.azure.com:443/"
COSMOS_KEY = "eZW6q45hUzd5mWZJ4WOqEPiTl4JLOCG8xTOvblqAsRt3zDuskQ9e8S5qmyLtSWita9m00hAwzxckACDbgCihNA=="
DATABASE_ID = "ToDoDatabase"      # <-- must match exactly in Data Explorer
CONTAINER_ID = "ToDoList"         # <-- must match exactly in Data Explorer
# Partition key for the container is assumed to be /category

# Connect to existing database + container (no create calls)
client = CosmosClient(COSMOS_ENDPOINT, credential=COSMOS_KEY)
database = client.get_database_client(DATABASE_ID)
container = database.get_container_client(CONTAINER_ID)

# ----------------------------
# Models
# ----------------------------
class BookIn(BaseModel):
    title: str = Field(..., min_length=1)
    author: str = Field(..., min_length=1)
    category: str = Field(..., min_length=1)  # must exist: partition key is /category

class BookUpdate(BaseModel):
    author: Optional[str] = Field(None, min_length=1)
    category: Optional[str] = Field(None, min_length=1)

def clean(doc: dict) -> dict:
    """Strip Cosmos system props before returning."""
    drop = {"_rid", "_self", "_etag", "_attachments", "_ts"}
    return {k: v for k, v in doc.items() if k not in drop}

# ----------------------------
# Helpers
# ----------------------------
def _find_by_title(title: str) -> Optional[dict]:
    """Return the first item with the given title (id==title) or None."""
    query = "SELECT TOP 1 * FROM c WHERE c.id=@id"  # id == title
    items = list(
        container.query_items(
            query,
            parameters=[{"name": "@id", "value": title}],
            enable_cross_partition_query=True,
        )
    )
    return items[0] if items else None

# ----------------------------
# Endpoints (create, list, delete, update)
# ----------------------------

@app.post("/books")
def create_book(book: BookIn):
    # Use title as id for simplicity (must be unique)
    item = book.model_dump()
    item["id"] = book.title
    try:
        container.create_item(item)
    except Exception as e:
        # Common cause: duplicate id (book with same title already exists)
        raise HTTPException(status_code=409, detail=str(e))
    return {"message": "created", "book": clean(item)}

@app.get("/books")
def list_books():
    items = list(container.query_items("SELECT * FROM c", enable_cross_partition_query=True))
    return [clean(i) for i in items]

@app.delete("/books/{title}")
def delete_book(title: str):
    # Find the item to get its partition key (category)
    doc = _find_by_title(title)
    if not doc:
        raise HTTPException(status_code=404, detail="Book not found")

    category = doc["category"]  # partition key value
    container.delete_item(item=title, partition_key=category)
    return {"message": "deleted", "title": title}

@app.put("/books/{title}")
def update_book(title: str, payload: BookUpdate):
    """
    Update a book by title (id). You can change author and/or category.
    If category changes, we still replace the full document with the same id.
    """
    # Fetch existing document
    old = _find_by_title(title)
    if not old:
        raise HTTPException(status_code=404, detail="Book not found")

    # Build the new body (keep id and title the same)
    new_doc = {
        "id": title,
        "title": title,
        "author": payload.author if payload.author is not None else old.get("author"),
        "category": payload.category if payload.category is not None else old.get("category"),
    }

    # Replace document (full-body replace)
    # Using the old doc object preserves ETag in some SDK versions, but we can pass new body directly as well.
    container.replace_item(item=old, body=new_doc)

    return {"message": "updated", "book": clean(new_doc)}

@app.get("/health")
def health():
    return {"status": "ok"}