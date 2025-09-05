from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os
import logging
from dotenv import load_dotenv

from .api.urls import router as urls_router
from .storage.factory import create_default_storage_service, StorageConfigurationError

# Load environment variables
load_dotenv()

# Set up logging
logger = logging.getLogger(__name__)

# Create FastAPI application
app = FastAPI(
    title="Civers Archive Web Interface",
    description="MVP for browsing and replaying archived versions of websites",
    version="1.0.0"
)

# Initialize storage service on startup
@app.on_event("startup")
async def startup_event():
    """Initialize storage service and attach to application state."""
    try:
        storage_service = create_default_storage_service()
        app.state.storage_service = storage_service
        logger.info("Storage service initialized successfully")
    except StorageConfigurationError as e:
        logger.error(f"Failed to initialize storage service: {e}")
        raise RuntimeError(f"Storage initialization failed: {e}") from e

# Configure Jinja2 templates
templates = Jinja2Templates(directory="templates")

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Include API routers
app.include_router(urls_router)

@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring and deployment validation"""
    return {
        "status": "healthy",
        "service": "civers-archive-web-interface",
        "version": "1.0.0"
    }

@app.get("/debug/cache/stats", include_in_schema=False, tags=["Debug"])
async def cache_stats(request: Request):
    """
    Get storage cache statistics.
    
    **Development and Testing Purpose Only**
    
    This endpoint provides internal cache statistics for development, 
    debugging, and testing purposes. It should not be used in production
    applications and may be removed or restricted in future versions.
    """
    storage_service = request.app.state.storage_service
    return storage_service.get_cache_stats()

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    """Home page - placeholder for now"""
    return templates.TemplateResponse("base.html", {
        "request": request,
        "title": "Civers Archive Web Interface"
    })

if __name__ == "__main__":
    import uvicorn
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    debug = os.getenv("DEBUG", "False").lower() == "true"
    
    uvicorn.run("app.main:app", host=host, port=port, reload=debug)