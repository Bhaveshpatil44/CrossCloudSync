from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool
from .replicator import replicate_file



app = FastAPI(
    title="AryaXAI Cross-Cloud Storage Replicator",
    description="Event-driven service for replicating S3 objects to GCS, optimized for AI data availability."
)

class ReplicationRequest(BaseModel):
    s3_bucket: str
    s3_key: str

@app.post("/v1/replicate", status_code=202)  # for 202 Accepted 
async def replicate_endpoint(request: ReplicationRequest):
    try:
        result = await replicate_file(request.s3_bucket, request.s3_key)
        return {"status": "success", "message": result}
    except ValueError as ve:  
        raise HTTPException(status_code=409, detail=str(ve))  
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")
