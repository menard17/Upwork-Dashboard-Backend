from beanie import Document, init_beanie
from motor.motor_asyncio import AsyncIOMotorClient
import datetime
from pydantic import BaseModel
from typing import List
from fastapi import FastAPI, HTTPException, BackgroundTasks
import asyncio
import logging

# Use DEBUG level for development
logging.basicConfig(level=logging.DEBUG)  
# MongoDB connection string
MONGODB_URI = "mongodb+srv://upwork2:upwork2@cluster0.g9bf3t1.mongodb.net/NursingAppDB-upwork2"

# Beanie document model
class ExceptionDocument(Document):
    error_name: str
    reason_message: str
    timestamp: datetime.datetime
    retry_count: int
    visit_id: str

# Pydantic models for request and response
class ExceptionCreate(BaseModel):
    error_name: str
    reason_message: str
    timestamp: datetime.datetime
    visit_id: str

class ExceptionResponse(BaseModel):
    id: str
    error_name: str
    reason_message: str
    timestamp: datetime.datetime
    retry_count: int
    visit_id: str

    @classmethod
    def from_document(cls, document: ExceptionDocument):
        return cls(
            id=str(document.id),  # Convert ObjectId to str
            error_name=document.error_name,
            reason_message=document.reason_message,
            timestamp=document.timestamp,
            retry_count=document.retry_count,
            visit_id=document.visit_id,
        )

# FastAPI app
app = FastAPI()

@app.on_event("startup")
async def on_startup():
    try:
        client = AsyncIOMotorClient(MONGODB_URI)
        await init_beanie(database=client.get_default_database(), document_models=[ExceptionDocument])
        logging.info("MongoDB connection established")
    except Exception as e:
        logging.error(f"Failed to initialize MongoDB: {str(e)}")
        # Optionally raise an exception or handle the error as needed
        raise RuntimeError("Failed to initialize MongoDB") from e

# Route to create an exception
@app.post("/exceptions/", response_model=ExceptionResponse)
async def create_exception(exception: ExceptionCreate):
    try:
        exception_doc = ExceptionDocument(**exception.dict(), retry_count=0)
        await exception_doc.insert()
        return ExceptionResponse.from_document(exception_doc)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Route to get the list of exceptions
@app.get("/exceptions/", response_model=List[ExceptionResponse])
async def get_exceptions():
    try:
        exceptions = await ExceptionDocument.find_all().to_list()
        return [ExceptionResponse.from_document(exception) for exception in exceptions]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Route to retry a job
@app.post("/exceptions/{id}/retry")
async def retry_exception(id: str, background_tasks: BackgroundTasks):
    try:
        exception = await ExceptionDocument.get(id)
        if not exception:
            raise HTTPException(status_code=404, detail="Exception not found")
        
        exception.retry_count += 1
        await exception.save()

        # Schedule retry job in background tasks
        background_tasks.add_task(process_job, exception)

        return {"message": "Retry initiated", "exception": exception}
    
    except HTTPException as http_exc:
        raise http_exc  # Re-raise HTTPException to propagate it with status code and detail
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to retry job: {str(e)}")

async def process_job(exception: ExceptionDocument):
    await asyncio.sleep(5)  # Simulate job processing
    # Add logic to retry the job and update the exception accordingly

# Route to notify the team
@app.post("/exceptions/{id}/notify")
async def notify_exception(id: str):
    try:
        exception = await ExceptionDocument.get(id)
        if not exception:
            raise HTTPException(status_code=404, detail="Exception not found")
        
        # Simulate sending an email or notification
        print(f"Notify team: Error {exception.error_name} failed with error {exception.reason_message}")

        return {"message": "Notification sent", "exception": exception}

    except HTTPException as http_exc:
        raise http_exc  # Re-raise HTTPException to propagate it with status code and detail
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to notify team: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
