# SSIS XML Analyzer

A full-stack application for analyzing SSIS DTSX XML files. Upload your SSIS package files to extract and visualize workflow activities, data flow components, connection managers, and detailed ETL pipeline information.

## Project Structure

```
SsisXmlAnalyzer/
├── ui/                    # Frontend (React + Node.js)
│   ├── client/           # React application
│   ├── server/           # Node.js/Express server
│   └── shared/           # Shared TypeScript schemas
│
├── api/                   # Backend (Python)
│   ├── api_server.py     # FastAPI server
│   └── requirements.txt  # Python dependencies
│
└── attached_assets/      # Sample SSIS files
```

## Quick Start

### 1. Install Dependencies

**UI (Node.js):**
```bash
cd ui
npm install
```

**API (Python):**
```bash
cd api
pip install -r requirements.txt
```

### 2. Run Both Servers

**Terminal 1 - Python API:**
```bash
cd api
python api_server.py
```

**Terminal 2 - UI Server:**
```bash
cd ui
npm run dev
```

### 3. Access the Application

- **Frontend**: http://localhost:5000
- **API**: http://localhost:8000/api/health

## Features

- 📤 **Upload DTSX Files**: Upload and parse SSIS XML package files
- 🔍 **Activity Analysis**: Extract and display all executable activities
- 🔄 **Data Flow Components**: Parse and visualize data flow tasks
- 🔌 **Connection Managers**: Extract connection string information
- 📊 **Detailed Properties**: View component properties, column mappings, and SQL commands

## Architecture

- **Frontend**: React + TypeScript + Vite (Port 5000)
- **Backend API**: Python FastAPI (Port 8000)
- **Communication**: UI server proxies requests to Python API

## Documentation

- **UI Documentation**: See [ui/README.md](ui/README.md)
- **API Documentation**: See [api/README.md](api/README.md)

## Development

Each component can be developed independently:

- **UI Development**: Work in the `ui/` folder
- **API Development**: Work in the `api/` folder

Both servers can run independently and communicate over HTTP.

## License

MIT
