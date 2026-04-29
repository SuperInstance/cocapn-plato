"""Server entry point — runs the Cocapn Plato FastAPI app."""
import uvicorn
from .routes import create_app

def main():
    app = create_app()
    uvicorn.run(app, host="0.0.0.0", port=8847)

if __name__ == "__main__":
    main()
