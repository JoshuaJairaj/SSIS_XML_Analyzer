import type { Express } from "express";
import { createServer, type Server } from "http";
import { storage } from "./storage";
import multer from "multer";
import FormData from "form-data";
import fetch from "node-fetch";

// Configure multer for file uploads
const upload = multer({ storage: multer.memoryStorage() });

export async function registerRoutes(app: Express): Promise<Server> {
  // Proxy endpoint to forward XML parsing to Python FastAPI server
  app.post("/api/parse-dtsx", upload.single('file'), async (req, res) => {
    try {
      if (!req.file) {
        return res.status(400).json({ 
          success: false, 
          message: "No file uploaded" 
        });
      }

      // Create form data with the uploaded file
      const formData = new FormData();
      formData.append('file', req.file.buffer, {
        filename: req.file.originalname,
        contentType: req.file.mimetype,
      });

      // Forward to Python FastAPI server
      const response = await fetch('http://localhost:8000/api/parse-dtsx', {
        method: 'POST',
        body: formData,
        headers: formData.getHeaders(),
      });

      // Parse response
      let data;
      try {
        data = await response.json();
      } catch (e) {
        // If response is not JSON, create error response
        const text = await response.text();
        return res.status(response.status || 500).json({
          detail: text || "Unknown error occurred"
        });
      }

      // Preserve the status code from the Python API response
      res.status(response.status).json(data);
    } catch (error) {
      console.error("Error proxying to Python FastAPI:", error);
      res.status(500).json({ 
        success: false, 
        message: "Error communicating with Python FastAPI server" 
      });
    }
  });

  // Proxy endpoint for Fabric mapping
  app.post("/api/map-to-fabric", async (req, res) => {
    try {
      const response = await fetch('http://localhost:8000/api/map-to-fabric', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(req.body),
      });

      // Check if response is JSON
      const contentType = response.headers.get('content-type') || '';
      let data;
      if (contentType.includes('application/json')) {
        data = await response.json();
      } else {
        // If not JSON, read as text to see the error
        const text = await response.text();
        console.error("Non-JSON response from FastAPI:", text.substring(0, 500));
        return res.status(response.status || 500).json({
          success: false,
          detail: `FastAPI returned non-JSON response: ${response.status} ${response.statusText}`,
          error: text.substring(0, 500)
        });
      }
      res.status(response.status).json(data);
    } catch (error) {
      console.error("Error proxying to Python FastAPI:", error);
      res.status(500).json({ 
        success: false, 
        detail: error instanceof Error ? error.message : "Error communicating with Python FastAPI server. Make sure the server is running on port 8000."
      });
    }
  });

  // Proxy endpoint for Fabric pipeline generation
  app.post("/api/generate-fabric-pipeline", async (req, res) => {
    try {
      const response = await fetch('http://localhost:8000/api/generate-fabric-pipeline', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(req.body),
      });

      // Check if response is JSON
      const contentType = response.headers.get('content-type') || '';
      let data;
      if (contentType.includes('application/json')) {
        data = await response.json();
      } else {
        const text = await response.text();
        console.error("Non-JSON response from FastAPI:", text.substring(0, 500));
        return res.status(response.status || 500).json({
          success: false,
          detail: `FastAPI returned non-JSON response: ${response.status} ${response.statusText}`,
          error: text.substring(0, 500)
        });
      }
      res.status(response.status).json(data);
    } catch (error) {
      console.error("Error proxying to Python FastAPI:", error);
      res.status(500).json({ 
        success: false, 
        detail: error instanceof Error ? error.message : "Error communicating with Python FastAPI server. Make sure the server is running on port 8000."
      });
    }
  });

  // Proxy endpoint for Fabric pipeline export
  app.post("/api/export-fabric-pipeline", async (req, res) => {
    try {
      const response = await fetch('http://localhost:8000/api/export-fabric-pipeline', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(req.body),
      });

      // For file downloads, preserve headers and stream response
      const contentType = response.headers.get('content-type') || '';
      if (contentType.includes('application/json')) {
        const data = await response.json();
        res.status(response.status).json(data);
      } else {
        // Stream the response for file downloads
        const buffer = await response.arrayBuffer();
        const contentDisposition = response.headers.get('content-disposition');
        if (contentDisposition) {
          res.setHeader('Content-Disposition', contentDisposition);
        }
        res.setHeader('Content-Type', contentType);
        res.status(response.status).send(Buffer.from(buffer));
      }
    } catch (error) {
      console.error("Error proxying to Python FastAPI:", error);
      res.status(500).json({ 
        success: false, 
        message: "Error communicating with Python FastAPI server" 
      });
    }
  });

  // Proxy endpoint for Fabric pipeline validation
  app.post("/api/validate-fabric-pipeline", async (req, res) => {
    try {
      const response = await fetch('http://localhost:8000/api/validate-fabric-pipeline', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(req.body),
      });

      // Check if response is JSON
      const contentType = response.headers.get('content-type') || '';
      let data;
      if (contentType.includes('application/json')) {
        data = await response.json();
      } else {
        const text = await response.text();
        console.error("Non-JSON response from FastAPI:", text.substring(0, 500));
        return res.status(response.status || 500).json({
          success: false,
          detail: `FastAPI returned non-JSON response: ${response.status} ${response.statusText}`,
          error: text.substring(0, 500)
        });
      }
      res.status(response.status).json(data);
    } catch (error) {
      console.error("Error proxying to Python FastAPI:", error);
      res.status(500).json({ 
        success: false, 
        detail: error instanceof Error ? error.message : "Error communicating with Python FastAPI server. Make sure the server is running on port 8000."
      });
    }
  });

  // Proxy: generate SQL for ADF metadata table (ControlTableIntegrated)
  app.post("/api/generate-control-table-sql", async (req, res) => {
    try {
      const response = await fetch('http://localhost:8000/api/generate-control-table-sql', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(req.body),
      });
      const data = await response.json().catch(() => ({}));
      res.status(response.status).json(data);
    } catch (error) {
      console.error("Error proxying generate-control-table-sql:", error);
      res.status(500).json({
        success: false,
        detail: error instanceof Error ? error.message : "Error communicating with Python FastAPI server.",
      });
    }
  });

  // Proxy: generate PySpark notebooks for Databricks (Silver/Gold)
  app.post("/api/generate-pyspark-notebooks", async (req, res) => {
    try {
      const response = await fetch('http://localhost:8000/api/generate-pyspark-notebooks', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(req.body),
      });
      const data = await response.json().catch(() => ({}));
      res.status(response.status).json(data);
    } catch (error) {
      console.error("Error proxying generate-pyspark-notebooks:", error);
      res.status(500).json({
        success: false,
        detail: error instanceof Error ? error.message : "Error communicating with Python FastAPI server.",
      });
    }
  });

  // Proxy: get migration package CLI commands
  app.post("/api/migration-package-commands", async (req, res) => {
    try {
      const response = await fetch("http://localhost:8000/api/migration-package-commands", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(req.body),
      });
      const data = await response.json().catch(() => ({}));
      res.status(response.status).json(data);
    } catch (error) {
      console.error("Error proxying migration-package-commands:", error);
      res.status(500).json({
        success: false,
        detail: error instanceof Error ? error.message : "Error communicating with Python FastAPI server.",
      });
    }
  });

  // Proxy: download migration package ZIP
  app.get("/api/migration-package/:packageId", async (req, res) => {
    try {
      const { packageId } = req.params;
      const response = await fetch(`http://localhost:8000/api/migration-package/${packageId}`, {
        method: "GET",
      });

      const contentType = response.headers.get("content-type") || "application/octet-stream";
      const contentDisposition = response.headers.get("content-disposition");
      if (contentDisposition) {
        res.setHeader("Content-Disposition", contentDisposition);
      }
      res.setHeader("Content-Type", contentType);

      const buffer = await response.arrayBuffer();
      res.status(response.status).send(Buffer.from(buffer));
    } catch (error) {
      console.error("Error proxying migration-package:", error);
      res.status(500).json({
        success: false,
        detail: error instanceof Error ? error.message : "Error communicating with Python FastAPI server.",
      });
    }
  });

  // Proxy endpoint for activity classification
  app.post("/api/classify-activity", async (req, res) => {
    try {
      const response = await fetch('http://localhost:8000/api/classify-activity', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(req.body),
      });

      // Check if response is JSON
      const contentType = response.headers.get('content-type') || '';
      let data;
      if (contentType.includes('application/json')) {
        data = await response.json();
      } else {
        const text = await response.text();
        console.error("Non-JSON response from FastAPI:", text.substring(0, 500));
        return res.status(response.status || 500).json({
          success: false,
          detail: `FastAPI returned non-JSON response: ${response.status} ${response.statusText}`,
          error: text.substring(0, 500)
        });
      }
      res.status(response.status).json(data);
    } catch (error) {
      console.error("Error proxying to Python FastAPI:", error);
      res.status(500).json({ 
        success: false, 
        detail: error instanceof Error ? error.message : "Error communicating with Python FastAPI server. Make sure the server is running on port 8000."
      });
    }
  });

  const httpServer = createServer(app);

  return httpServer;
}
