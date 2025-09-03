from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Create FastAPI application
app = FastAPI(
    title="Civers Archive Web Interface",
    description="MVP for browsing and replaying archived versions of websites",
    version="1.0.0"
)

# Configure Jinja2 templates
templates = Jinja2Templates(directory="templates")

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring and deployment validation"""
    return {
        "status": "healthy",
        "service": "civers-archive-web-interface",
        "version": "1.0.0"
    }

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