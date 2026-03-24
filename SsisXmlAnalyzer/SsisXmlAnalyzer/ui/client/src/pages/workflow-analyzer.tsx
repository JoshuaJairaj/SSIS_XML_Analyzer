import { useState } from "react";
import { useEffect } from "react";
import { Upload, Search, Database, Code, GitBranch, FileText, PlayCircle, ArrowLeft, FileCode, Download, Cloud, AlertTriangle, CheckCircle2, XCircle, Loader2, Lock, Clock, ArrowRight, Layers, FileDown, Copy } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "@/components/ui/accordion";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Skeleton } from "@/components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { useToast } from "@/hooks/use-toast";
import type { ParsedPackage, Activity, MappingTrace, ConversionSummary } from "@shared/schema";

export default function WorkflowAnalyzer() {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [parsedData, setParsedData] = useState<ParsedPackage | null>(null);
  const [packageId, setPackageId] = useState<string | null>(null);
  const [originalFilename, setOriginalFilename] = useState<string | null>(null);
  const [selectedActivity, setSelectedActivity] = useState<Activity | null>(null);
  const [searchQuery, setSearchQuery] = useState("");
  const [isUploading, setIsUploading] = useState(false);
  const [xmlContent, setXmlContent] = useState<string>("");
  const [pendingUpload, setPendingUpload] = useState<(() => void) | null>(null);
  const [mappingTrace, setMappingTrace] = useState<MappingTrace | null>(null);
  const [fabricPipeline, setFabricPipeline] = useState<any>(null);
  const [conversionSummary, setConversionSummary] = useState<ConversionSummary | null>(null);
  const [isMapping, setIsMapping] = useState(false);
  const [isGenerating, setIsGenerating] = useState(false);
  const [activeTab, setActiveTab] = useState("parsed");
  const [unityCatalogSql, setUnityCatalogSql] = useState<string | null>(null);
  const [unityCatalogNotebooks, setUnityCatalogNotebooks] = useState<Record<string, string> | null>(null);
  const [cliCommands, setCliCommands] = useState<Array<{ pipeline_file: string; job_file: string; pipeline_cmd: string; job_cmd: string; table_key: string }> | null>(null);
  const [isLoadingUnityCatalog, setIsLoadingUnityCatalog] = useState(false);
  const { toast } = useToast();

  const handleFileSelect = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (file) {
      // Validate file extension
      if (file.name.endsWith('.xml') || file.name.endsWith('.dtsx')) {
        // Clear previous parsed data when a new file is selected
        setParsedData(null);
        setPackageId(null);
        setOriginalFilename(null);
        setSelectedActivity(null);
        setSearchQuery("");
        setXmlContent("");
        setMappingTrace(null);
        setFabricPipeline(null);
        setConversionSummary(null);
        setUnityCatalogSql(null);
        setUnityCatalogNotebooks(null);
        setSelectedFile(file);
      } else {
        toast({
          title: "Invalid file type",
          description: "Please upload a .xml or .dtsx file",
          variant: "destructive",
        });
        // Reset file input on invalid file
        event.target.value = '';
      }
    }
  };

  const handleDrop = (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
    const file = event.dataTransfer.files?.[0];
    if (file && (file.name.endsWith('.xml') || file.name.endsWith('.dtsx'))) {
      // Clear previous parsed data when a new file is dropped
      setParsedData(null);
      setPackageId(null);
      setOriginalFilename(null);
      setSelectedActivity(null);
      setSearchQuery("");
      setXmlContent("");
      setMappingTrace(null);
      setFabricPipeline(null);
      setConversionSummary(null);
      setUnityCatalogSql(null);
      setUnityCatalogNotebooks(null);
      setSelectedFile(file);
    } else {
      toast({
        title: "Invalid file type",
        description: "Please upload a .xml or .dtsx file",
        variant: "destructive",
      });
    }
  };

  const handleDragOver = (event: React.DragEvent<HTMLDivElement>) => {
    event.preventDefault();
  };

  const performUpload = async () => {
    if (!selectedFile) return;

    setIsUploading(true);
    try {
      // Read file content as text for XML display (fallback if API doesn't return formatted XML)
      const fileText = await selectedFile.text();
      setXmlContent(fileText);

      const formData = new FormData();
      formData.append('file', selectedFile);

      const response = await fetch('/api/parse-dtsx', {
        method: 'POST',
        body: formData,
      });

      let result;
      try {
        result = await response.json();
      } catch (e) {
        // If response is not JSON, it might be an error
        if (!response.ok) {
          throw new Error(`Server error: ${response.status} ${response.statusText}`);
        }
        throw new Error('Failed to parse server response');
      }

      // Handle errors
      if (!response.ok) {
        const errorMessage = result.detail || result.message || JSON.stringify(result);
        toast({
          title: "Parse error",
          description: errorMessage || "Failed to parse XML file",
          variant: "destructive",
        });
        return;
      }

      if (result.success && result.data) {
        setParsedData(result.data);
        setPackageId(typeof result.packageId === "string" ? result.packageId : null);
        setOriginalFilename(typeof result.originalFilename === "string" ? result.originalFilename : null);
        // Use formatted XML from API if available, otherwise use file text
        if (result.formattedXml) {
          setXmlContent(result.formattedXml);
        }
        toast({
          title: "File parsed successfully",
          description: `Found ${result.data.activities.length} activities in workflow`,
        });
      } else {
        toast({
          title: "Parse error",
          description: result.message || result.detail || "Failed to parse XML file",
          variant: "destructive",
        });
      }
    } catch (error: any) {
      toast({
        title: "Upload failed",
        description: error.message || "An error occurred while uploading the file",
        variant: "destructive",
      });
    } finally {
      setIsUploading(false);
    }
  };

  const handleUpload = async () => {
    await performUpload();
  };


  const handleClear = () => {
    setSelectedFile(null);
    setParsedData(null);
    setPackageId(null);
    setOriginalFilename(null);
    setSelectedActivity(null);
    setSearchQuery("");
    setXmlContent("");
    setMappingTrace(null);
    setFabricPipeline(null);
    setConversionSummary(null);
    setUnityCatalogSql(null);
    setUnityCatalogNotebooks(null);
    // Reset file input so the same file can be selected again
    const fileInput = document.getElementById('file-input') as HTMLInputElement;
    if (fileInput) {
      fileInput.value = '';
    }
  };

  // Reset Unity Catalog artifacts when parsed data changes (new file parsed)
  useEffect(() => {
    if (parsedData) {
      setUnityCatalogSql(null);
      setUnityCatalogNotebooks(null);
      setCliCommands(null);
    }
  }, [parsedData]);

  // Fetch Azure Unity Catalog artifacts when tab is selected and we have parsed data
  useEffect(() => {
    if (activeTab !== "unity-catalog" || !parsedData) return;
    if (unityCatalogSql !== null && unityCatalogNotebooks !== null && cliCommands !== null) return; // already loaded

    let cancelled = false;
    const fetchUnityCatalog = async () => {
      setIsLoadingUnityCatalog(true);
      try {
        const payload = { data: parsedData };
        const [sqlRes, notebooksRes, commandsRes] = await Promise.all([
          fetch("/api/generate-control-table-sql", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          }),
          fetch("/api/generate-pyspark-notebooks", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          }),
          fetch("/api/migration-package-commands", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
          }),
        ]);
        if (cancelled) return;
        const sqlJson = await sqlRes.json().catch(() => ({}));
        const notebooksJson = await notebooksRes.json().catch(() => ({}));
        const commandsJson = await commandsRes.json().catch(() => ({}));
        if (sqlJson.success && sqlJson.sqlScript != null) {
          setUnityCatalogSql(sqlJson.sqlScript);
        } else {
          setUnityCatalogSql("-- No copy activities found or error generating SQL.");
        }
        if (notebooksJson.success && notebooksJson.notebooks && typeof notebooksJson.notebooks === "object") {
          setUnityCatalogNotebooks(notebooksJson.notebooks);
        } else {
          setUnityCatalogNotebooks({});
        }
        if (commandsJson.success && Array.isArray(commandsJson.commands)) {
          setCliCommands(commandsJson.commands);
        } else {
          setCliCommands([]);
        }
      } catch (e) {
        if (!cancelled) {
          toast({
            title: "Unity Catalog load failed",
            description: e instanceof Error ? e.message : "Failed to load SQL and notebooks.",
            variant: "destructive",
          });
          setUnityCatalogSql("-- Error loading metadata SQL.");
          setUnityCatalogNotebooks({});
          setCliCommands([]);
        }
      } finally {
        if (!cancelled) setIsLoadingUnityCatalog(false);
      }
    };
    fetchUnityCatalog();
    return () => { cancelled = true; };
  }, [activeTab, parsedData, unityCatalogSql, unityCatalogNotebooks, cliCommands]);

  const handleDownloadSql = () => {
    if (!unityCatalogSql) return;
    const blob = new Blob([unityCatalogSql], { type: "application/sql" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "ControlTableIntegrated_Inserts.sql";
    a.click();
    URL.revokeObjectURL(url);
    toast({ title: "Downloaded", description: "ControlTableIntegrated_Inserts.sql" });
  };

  const handleCopyCliCommand = (text: string) => {
    navigator.clipboard.writeText(text).then(
      () => toast({ title: "Copied", description: "Command copied to clipboard" }),
      () => toast({ title: "Copy failed", variant: "destructive" })
    );
  };

  const handleDownloadNotebook = (filename: string, content: string) => {
    const blob = new Blob([content], { type: "text/x-python" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
    toast({ title: "Downloaded", description: filename });
  };

  const handleDownloadMigrationPackage = async () => {
    if (!packageId) {
      toast({
        title: "Migration package unavailable",
        description: "Parse a package first to generate a migration bundle.",
        variant: "destructive",
      });
      return;
    }
    try {
      const res = await fetch(`/api/migration-package/${packageId}`, { method: "GET" });
      if (!res.ok) {
        const msg = await res.text().catch(() => "");
        throw new Error(msg || `Request failed: ${res.status}`);
      }
      const blob = await res.blob();
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      let baseName = originalFilename || parsedData?.metadata?.name || parsedData?.metadata?.objectName || "SSISPackage";
      const ext = baseName.toLowerCase().endsWith(".dtsx") ? ".dtsx" : baseName.toLowerCase().endsWith(".xml") ? ".xml" : null;
      if (ext) baseName = baseName.slice(0, -ext.length);
      a.download = `${baseName}_migration_package.zip`;
      a.click();
      URL.revokeObjectURL(url);
      toast({ title: "Downloaded", description: a.download });
    } catch (e: any) {
      toast({
        title: "Download failed",
        description: e?.message || "Failed to download migration package",
        variant: "destructive",
      });
    }
  };

  const handleMapToFabric = async () => {
    if (!parsedData) return;
    
    setIsMapping(true);
    try {
      const response = await fetch('/api/map-to-fabric', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(parsedData),
      });
      
      // Check content type before parsing
      const contentType = response.headers.get('content-type') || '';
      let result;
      
      if (contentType.includes('application/json')) {
        result = await response.json();
      } else {
        // If not JSON, read as text
        const text = await response.text();
        throw new Error(`Server returned non-JSON response (${response.status}): ${text.substring(0, 200)}`);
      }
      
      if (!response.ok) {
        throw new Error(result.detail || result.message || 'Failed to map to Fabric');
      }
      
      if (result.success && result.mappingTrace) {
        setMappingTrace(result.mappingTrace);
        toast({
          title: "Mapping successful",
          description: `Mapped ${result.mappingTrace.diagnostics.mappedActivities} activities to Fabric`,
        });
      }
    } catch (error: any) {
      console.error("Map to Fabric error:", error);
      toast({
        title: "Mapping failed",
        description: error.message || "Failed to map package to Fabric. Make sure the FastAPI server is running on port 8000.",
        variant: "destructive",
      });
    } finally {
      setIsMapping(false);
    }
  };

  const handleGeneratePipeline = async () => {
    if (!parsedData) return;
    
    setIsGenerating(true);
    try {
      const response = await fetch('/api/generate-fabric-pipeline', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          package_data: parsedData,
          mapping_trace: mappingTrace,
        }),
      });
      
      // Check content type before parsing
      const contentType = response.headers.get('content-type') || '';
      let result;
      
      if (contentType.includes('application/json')) {
        result = await response.json();
      } else {
        // If not JSON, read as text
        const text = await response.text();
        throw new Error(`Server returned non-JSON response (${response.status}): ${text.substring(0, 200)}`);
      }
      
      if (!response.ok) {
        throw new Error(result.detail || result.message || 'Failed to generate pipeline');
      }
      
      if (result.success) {
        setFabricPipeline(result.pipeline);
        setConversionSummary(result.summary);
        setMappingTrace(result.mappingTrace);
        toast({
          title: "Pipeline generated",
          description: `Generated Fabric pipeline: ${result.pipeline.name}`,
        });
      }
    } catch (error: any) {
      console.error("Generate pipeline error:", error);
      toast({
        title: "Pipeline generation failed",
        description: error.message || "Failed to generate Fabric pipeline. Make sure the FastAPI server is running on port 8000.",
        variant: "destructive",
      });
    } finally {
      setIsGenerating(false);
    }
  };

  const handleDownloadPipeline = async () => {
    if (!fabricPipeline) return;
    
    try {
      const response = await fetch('/api/export-fabric-pipeline', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          pipeline: fabricPipeline,
          filename: `${fabricPipeline.name || 'fabric_pipeline'}.json`,
        }),
      });
      
      if (!response.ok) {
        throw new Error('Failed to export pipeline');
      }
      
      const blob = await response.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = `${fabricPipeline.name || 'fabric_pipeline'}.json`;
      document.body.appendChild(a);
      a.click();
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
      
      toast({
        title: "Download started",
        description: "Fabric pipeline JSON downloaded",
      });
    } catch (error: any) {
      toast({
        title: "Download failed",
        description: error.message || "Failed to download pipeline",
        variant: "destructive",
      });
    }
  };

  const filteredActivities = parsedData?.activities.filter(activity =>
    activity.name.toLowerCase().includes(searchQuery.toLowerCase()) ||
    activity.type.toLowerCase().includes(searchQuery.toLowerCase())
  ) || [];

  const getActivityIcon = (type: string) => {
    if (type.includes('Pipeline') || type.includes('DataFlow')) {
      return <GitBranch className="w-4 h-4" />;
    } else if (type.includes('SQL')) {
      return <Database className="w-4 h-4" />;
    } else if (type.includes('Script')) {
      return <Code className="w-4 h-4" />;
    } else if (type.includes('Execute Package') || type.includes('PackageTask')) {
      return <Layers className="w-4 h-4" />;
    }
    return <PlayCircle className="w-4 h-4" />;
  };

  const getActivityTypeName = (type: string) => {
    if (type.includes('Pipeline')) return 'Data Flow Task';
    if (type.includes('SQL')) return 'Execute SQL Task';
    if (type.includes('Script')) return 'Script Task';
    if (type.includes('Execute Package') || type.includes('PackageTask')) return 'Execute Package Task';
    return type.split('.').pop() || type;
  };

  return (
    <>
      <div className="flex flex-col h-screen bg-background">
        {/* Header */}
        <header className="h-14 bg-gradient-to-r from-blue-600 to-blue-500 shadow-md flex items-center justify-between px-6">
          <div className="flex items-center gap-3">
            <FileText className="w-5 h-5 text-white" />
            <h1 className="text-lg font-semibold text-white">SSIS Workflow Analyzer</h1>
          </div>
          {parsedData && (
            <Button
              variant="outline"
              size="sm"
              onClick={handleClear}
              data-testid="button-clear"
              className="bg-white text-blue-600 hover:bg-blue-50 border-0 font-medium"
            >
              Clear
            </Button>
          )}
        </header>

        <div className="flex flex-1 overflow-hidden">
          {/* Sidebar */}
          <aside className="w-80 border-r border-border bg-sidebar flex flex-col overflow-hidden">
            <div className="p-4 flex flex-col flex-1 min-h-0">
              {/* Upload Dropzone */}
              <div
                onDrop={handleDrop}
                onDragOver={handleDragOver}
                className="border-2 border-dashed border-border rounded-lg p-6 text-center hover-elevate bg-background"
                data-testid="dropzone-upload"
              >
                <Upload className="w-8 h-8 mx-auto mb-2 text-muted-foreground" />
                <p className="text-sm font-medium text-foreground mb-1">
                  {selectedFile ? selectedFile.name : 'Drop DTSX or XML file here'}
                </p>
                <p className="text-xs text-muted-foreground mb-3">
                  or click to browse (.dtsx or .xml)
                </p>
                <div className="flex flex-col gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => document.getElementById('file-input')?.click()}
                    data-testid="button-browse"
                  >
                    Browse Files
                  </Button>
                  {selectedFile && (
                    <Button
                      size="sm"
                      onClick={handleUpload}
                      disabled={isUploading}
                      data-testid="button-upload"
                    >
                      {isUploading ? 'Parsing...' : parsedData ? 'Re-parse File' : 'Parse File'}
                    </Button>
                  )}
                </div>
                <input
                  id="file-input"
                  type="file"
                  accept=".xml,.dtsx"
                  onChange={handleFileSelect}
                  className="hidden"
                  data-testid="input-file"
                />
              </div>

              {/* Search Bar */}
              {parsedData && (
                <div className="mt-4 flex-shrink-0">
                  <div className="relative">
                    <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 w-4 h-4 text-muted-foreground" />
                    <Input
                      type="text"
                      placeholder="Search activities..."
                      value={searchQuery}
                      onChange={(e) => setSearchQuery(e.target.value)}
                      className="pl-9"
                      data-testid="input-search"
                    />
                  </div>
                </div>
              )}

              {/* Activity List */}
              {parsedData && (
                <ScrollArea className="mt-4 flex-1 min-h-0">
                  <div className="space-y-2 pr-4">
                    {filteredActivities.map((activity) => (
                      <button
                        key={activity.id}
                        onClick={() => setSelectedActivity(activity)}
                        className={`w-full text-left p-3 rounded-md border transition-colors ${
                          selectedActivity?.id === activity.id
                            ? 'bg-primary/10 border-primary'
                            : 'bg-background border-border hover:bg-muted'
                        }`}
                        data-testid={`activity-${activity.id}`}
                      >
                        <div className="flex items-center gap-2 mb-1">
                          {getActivityIcon(activity.type)}
                          <p className="text-sm font-semibold text-foreground">{activity.name}</p>
                        </div>
                        <Badge variant="secondary" className="text-xs">
                          {getActivityTypeName(activity.type)}
                        </Badge>
                      </button>
                    ))}
                  </div>
                </ScrollArea>
              )}
            </div>
          </aside>

          {/* Main Content */}
          <main className="flex-1 overflow-hidden">
            <ScrollArea className="h-full">
              <div className="p-6">
                {!parsedData ? (
                  <div className="flex flex-col items-center justify-center h-[calc(100vh-8rem)] text-center">
                    <FileText className="w-16 h-16 text-muted-foreground mb-4" />
                    <h2 className="text-xl font-semibold text-foreground mb-2">
                      Upload SSIS Package File to Begin
                    </h2>
                    <p className="text-sm text-muted-foreground max-w-md">
                      Upload an SSIS package file (.dtsx) or XML export file (.xml) to analyze the workflow activities,
                      data flow components, and extract detailed information about each step.
                    </p>
                  </div>
                ) : (
                  <Tabs defaultValue="parsed" value={activeTab} onValueChange={setActiveTab} className="w-full">
                    <TabsList className="mb-4">
                      <TabsTrigger value="parsed" className="gap-2">
                        <FileText className="w-4 h-4" />
                        Parsed View
                      </TabsTrigger>
                      <TabsTrigger value="fabric" className="gap-2">
                        <Cloud className="w-4 h-4" />
                        Fabric Mapping
                      </TabsTrigger>
                      <TabsTrigger value="xml" className="gap-2">
                        <FileCode className="w-4 h-4" />
                        XML View
                      </TabsTrigger>
                      <TabsTrigger value="unity-catalog" className="gap-2">
                        <Layers className="w-4 h-4" />
                        Azure Unity Catalog
                      </TabsTrigger>
                    </TabsList>
                    
                    <TabsContent value="parsed" className="mt-0">
                      {!selectedActivity ? (
                        <div>
                          {/* Workflow Overview */}
                          <div className="mb-6">
                            <h2 className="text-2xl font-semibold text-foreground mb-2">
                              {parsedData.metadata.objectName}
                            </h2>
                            <p className="text-sm text-muted-foreground">
                              {parsedData.metadata.description || 'SSIS Package Workflow'}
                            </p>
                          </div>

                          {/* Package Metadata */}
                          <Card className="mb-6">
                            <CardHeader>
                              <CardTitle>Package Information</CardTitle>
                            </CardHeader>
                            <CardContent className="grid grid-cols-2 gap-4">
                              <div>
                                <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-1">
                                  Package Name
                                </p>
                                <p className="text-sm font-mono text-foreground">
                                  {parsedData.metadata.name}
                                </p>
                              </div>
                              <div>
                                <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-1">
                                  Created By
                                </p>
                                <p className="text-sm font-mono text-foreground">
                                  {parsedData.metadata.creator}
                                </p>
                              </div>
                              <div>
                                <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-1">
                                  Creation Date
                                </p>
                                <p className="text-sm font-mono text-foreground">
                                  {parsedData.metadata.creationDate}
                                </p>
                              </div>
                              <div>
                                <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-1">
                                  Version
                                </p>
                                <p className="text-sm font-mono text-foreground">
                                  {parsedData.metadata.versionBuild || 'N/A'}
                                </p>
                              </div>
                            </CardContent>
                            
                            {/* Component Summary */}
                            {parsedData.componentSummary && (
                              <CardContent className="border-t border-border pt-4 mt-4">
                                <div className="mb-4">
                                  <h3 className="text-sm font-semibold text-foreground mb-3">Component Summary</h3>
                                  
                                  {/* Activity Type Counts */}
                                  {parsedData.componentSummary.activityTypeCounts && 
                                   parsedData.componentSummary.activityTypeCounts.length > 0 && (
                                    <div className="mb-4">
                                      <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2">
                                        Activity Types ({parsedData.componentSummary.totalActivities} total)
                                      </p>
                                      <div className="overflow-x-auto">
                                        <table className="w-full text-xs">
                                          <thead>
                                            <tr className="border-b border-border">
                                              <th className="text-left py-2 px-2 font-medium text-muted-foreground">Component Name</th>
                                              <th className="text-right py-2 px-2 font-medium text-muted-foreground">Count</th>
                                            </tr>
                                          </thead>
                                          <tbody>
                                            {parsedData.componentSummary.activityTypeCounts.map((item, idx) => (
                                              <tr key={idx} className="border-b border-border/50 last:border-0">
                                                <td className="py-2 px-2 font-semibold text-foreground">{item.name}</td>
                                                <td className="py-2 px-2 text-right">
                                                  <Badge variant="secondary" className="text-xs">
                                                    {item.count}
                                                  </Badge>
                                                </td>
                                              </tr>
                                            ))}
                                          </tbody>
                                        </table>
                                      </div>
                                    </div>
                                  )}
                                  
                                  {/* Data Flow Component Type Counts */}
                                  {parsedData.componentSummary.dataFlowComponentTypeCounts && 
                                   parsedData.componentSummary.dataFlowComponentTypeCounts.length > 0 && (
                                    <div>
                                      <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2">
                                        Data Flow Components ({parsedData.componentSummary.totalDataFlowComponents} total)
                                      </p>
                                      <div className="overflow-x-auto">
                                        <table className="w-full text-xs">
                                          <thead>
                                            <tr className="border-b border-border">
                                              <th className="text-left py-2 px-2 font-medium text-muted-foreground">Component Name</th>
                                              <th className="text-right py-2 px-2 font-medium text-muted-foreground">Count</th>
                                            </tr>
                                          </thead>
                                          <tbody>
                                            {parsedData.componentSummary.dataFlowComponentTypeCounts.map((item, idx) => (
                                              <tr key={idx} className="border-b border-border/50 last:border-0">
                                                <td className="py-2 px-2 font-semibold text-foreground">{item.name}</td>
                                                <td className="py-2 px-2 text-right">
                                                  <Badge variant="secondary" className="text-xs">
                                                    {item.count}
                                                  </Badge>
                                                </td>
                                              </tr>
                                            ))}
                                          </tbody>
                                        </table>
                                      </div>
                                    </div>
                                  )}
                                  
                                  {/* Empty State */}
                                  {(!parsedData.componentSummary.activityTypeCounts || 
                                    parsedData.componentSummary.activityTypeCounts.length === 0) &&
                                   (!parsedData.componentSummary.dataFlowComponentTypeCounts || 
                                    parsedData.componentSummary.dataFlowComponentTypeCounts.length === 0) && (
                                    <p className="text-xs text-muted-foreground py-2">
                                      No components found in package.
                                    </p>
                                  )}
                                </div>
                              </CardContent>
                            )}
                          </Card>

                          {/* Package-wide referenced tables (all SQL: Execute SQL, Data Flow) */}
                          {parsedData.packageReferencedTables && parsedData.packageReferencedTables.length > 0 && (
                            <Card className="mb-6">
                              <CardHeader>
                                <CardTitle className="flex items-center gap-2">
                                  Referenced Tables (package-wide)
                                  <Badge variant="secondary" className="text-xs">
                                    {parsedData.packageReferencedTables.length}
                                  </Badge>
                                </CardTitle>
                                <CardDescription>
                                  Tables detected from all SQL in this package (Execute SQL tasks, Data Flow sources, destinations, Lookup, etc.)
                                </CardDescription>
                              </CardHeader>
                              <CardContent>
                                <div className="flex flex-wrap gap-2 mb-4">
                                  {parsedData.packageReferencedTables.map((tableRef, idx) => (
                                    <Badge key={idx} variant="secondary" className="text-xs font-mono break-all cursor-help" title={tableRef.fullName}>
                                      {tableRef.fullName}
                                    </Badge>
                                  ))}
                                </div>
                                {parsedData.packageReferencedTablesDetailed && parsedData.packageReferencedTablesDetailed.length > 0 && (
                                  <div className="border-t border-border pt-4 space-y-2 max-h-64 overflow-y-auto">
                                    <p className="text-xs font-medium text-muted-foreground uppercase tracking-wide">Where referenced</p>
                                    {parsedData.packageReferencedTablesDetailed.map((row, idx) => (
                                      <div key={idx} className="text-xs border border-border rounded p-2 bg-muted/30">
                                        <span className="font-mono font-medium text-foreground">{row.table.fullName}</span>
                                        <ul className="list-disc list-inside mt-1 text-muted-foreground">
                                          {row.referencedFrom.map((src, j) => (
                                            <li key={j}>{src}</li>
                                          ))}
                                        </ul>
                                      </div>
                                    ))}
                                  </div>
                                )}
                              </CardContent>
                            </Card>
                          )}

                          {/* Connection Managers */}
                          {parsedData.connectionManagers.length > 0 && (
                            <Card className="mb-6">
                              <CardHeader>
                                <CardTitle>Connection Managers</CardTitle>
                                <CardDescription>
                                  {parsedData.connectionManagers.length} connection(s) configured
                                </CardDescription>
                              </CardHeader>
                              <CardContent className="space-y-4">
                                {parsedData.connectionManagers.map((conn) => (
                                  <div key={conn.id} className="border border-border rounded-md p-4 bg-muted/30">
                                    <div className="flex items-center gap-2 mb-3">
                                      <Database className="w-4 h-4 text-primary" />
                                      <p className="text-sm font-semibold text-foreground">
                                        {conn.name}
                                      </p>
                                      <Badge variant="outline" className="text-xs">
                                        {conn.creationName}
                                      </Badge>
                                    </div>
                                    <div className="grid grid-cols-2 gap-3 text-xs">
                                      {conn.dataSource && (
                                        <div>
                                          <span className="font-medium uppercase tracking-wide text-muted-foreground">
                                            Data Source:
                                          </span>
                                          <p className="font-mono text-foreground mt-0.5">{conn.dataSource}</p>
                                        </div>
                                      )}
                                      {conn.initialCatalog && (
                                        <div>
                                          <span className="font-medium uppercase tracking-wide text-muted-foreground">
                                            Database:
                                          </span>
                                          <p className="font-mono text-foreground mt-0.5">{conn.initialCatalog}</p>
                                        </div>
                                      )}
                                      {conn.provider && (
                                        <div>
                                          <span className="font-medium uppercase tracking-wide text-muted-foreground">
                                            Provider:
                                          </span>
                                          <p className="font-mono text-foreground mt-0.5">{conn.provider}</p>
                                        </div>
                                      )}
                                      {conn.userId && (
                                        <div>
                                          <span className="font-medium uppercase tracking-wide text-muted-foreground">
                                            User ID:
                                          </span>
                                          <p className="font-mono text-foreground mt-0.5">{conn.userId}</p>
                                        </div>
                                      )}
                                    </div>
                                  </div>
                                ))}
                              </CardContent>
                            </Card>
                          )}

                          {/* Connections Usage Map */}
                          {parsedData.connectionsUsageMap && parsedData.connectionsUsageMap.allConnections && parsedData.connectionsUsageMap.allConnections.length > 0 && (
                            <Card className="mb-6">
                              <CardHeader>
                                <CardTitle className="flex items-center gap-2">
                                  Connections Usage Map
                                  <Badge variant="secondary" className="text-xs">
                                    {parsedData.connectionsUsageMap.usedConnections}/{parsedData.connectionsUsageMap.totalConnections}
                                  </Badge>
                                </CardTitle>
                                <CardDescription>
                                  Detailed view of where each connection is used across the package
                                </CardDescription>
                              </CardHeader>
                              <CardContent className="space-y-4">
                                {/* Summary Stats */}
                                <div className="grid grid-cols-3 gap-3 mb-4">
                                  <div className="p-3 bg-green-50 dark:bg-green-950 rounded-md border border-green-200 dark:border-green-800">
                                    <p className="text-xs text-green-800 dark:text-green-200 font-medium uppercase tracking-wide">Used</p>
                                    <p className="text-2xl font-bold text-green-700 dark:text-green-300">{parsedData.connectionsUsageMap.usedConnections}</p>
                                  </div>
                                  <div className="p-3 bg-amber-50 dark:bg-amber-950 rounded-md border border-amber-200 dark:border-amber-800">
                                    <p className="text-xs text-amber-800 dark:text-amber-200 font-medium uppercase tracking-wide">Total</p>
                                    <p className="text-2xl font-bold text-amber-700 dark:text-amber-300">{parsedData.connectionsUsageMap.totalConnections}</p>
                                  </div>
                                  <div className="p-3 bg-red-50 dark:bg-red-950 rounded-md border border-red-200 dark:border-red-800">
                                    <p className="text-xs text-red-800 dark:text-red-200 font-medium uppercase tracking-wide">Unused</p>
                                    <p className="text-2xl font-bold text-red-700 dark:text-red-300">{parsedData.connectionsUsageMap.unusedConnectionCount}</p>
                                  </div>
                                </div>

                                {/* Connections with Usage Details */}
                                <div className="space-y-3">
                                  {parsedData.connectionsUsageMap.allConnections.map((conn) => (
                                    <div
                                      key={conn.id}
                                      className={`p-4 rounded-md border ${
                                        conn.usageCount > 0
                                          ? 'bg-blue-50 dark:bg-blue-950 border-blue-200 dark:border-blue-800'
                                          : 'bg-red-50 dark:bg-red-950 border-red-200 dark:border-red-800'
                                      }`}
                                    >
                                      <div className="flex items-start justify-between mb-2">
                                        <div>
                                          <p className="font-semibold text-foreground">{conn.name}</p>
                                          <p className="text-xs text-muted-foreground">{conn.creationName}</p>
                                        </div>
                                        <div className="flex gap-2">
                                          {conn.usageCount > 0 ? (
                                            <>
                                              <Badge variant="secondary" className="bg-blue-100 dark:bg-blue-900 text-blue-800 dark:text-blue-100">
                                                Used {conn.usageCount}x
                                              </Badge>
                                              {conn.usedInActivities > 0 && (
                                                <Badge variant="outline" className="text-xs">
                                                  Activity: {conn.usedInActivities}
                                                </Badge>
                                              )}
                                              {conn.usedInComponents > 0 && (
                                                <Badge variant="outline" className="text-xs">
                                                  Component: {conn.usedInComponents}
                                                </Badge>
                                              )}
                                            </>
                                          ) : (
                                            <Badge variant="destructive" className="bg-red-100 dark:bg-red-900 text-red-800 dark:text-red-100">
                                              Unused
                                            </Badge>
                                          )}
                                        </div>
                                      </div>

                                      {/* Usage Details */}
                                      {conn.usageDetails && conn.usageDetails.length > 0 && (
                                        <div className="mt-3 space-y-2 text-xs">
                                          <p className="text-muted-foreground font-medium">Usage Locations:</p>
                                          {conn.usageDetails.map((usage, idx) => (
                                            <div key={idx} className="ml-2 p-2 bg-background dark:bg-slate-900 rounded border border-border">
                                              <p className="font-mono text-foreground text-xs mb-1">
                                                {usage.locationInPackage}
                                              </p>
                                              {usage.activityName && (
                                                <p className="text-muted-foreground text-xs">
                                                  Activity: <span className="font-medium">{usage.activityName}</span>
                                                </p>
                                              )}
                                              {usage.componentName && (
                                                <p className="text-muted-foreground text-xs">
                                                  Component: <span className="font-medium">{usage.componentName}</span> ({usage.componentType})
                                                </p>
                                              )}
                                            </div>
                                          ))}
                                        </div>
                                      )}

                                      {/* Connection Details */}
                                      {(conn.dataSource || conn.initialCatalog || conn.provider) && (
                                        <div className="mt-3 grid grid-cols-2 gap-2 pt-3 border-t border-border text-xs">
                                          {conn.dataSource && (
                                            <div>
                                              <span className="text-muted-foreground font-medium">DataSource:</span>
                                              <p className="font-mono text-foreground text-xs break-all">{conn.dataSource}</p>
                                            </div>
                                          )}
                                          {conn.initialCatalog && (
                                            <div>
                                              <span className="text-muted-foreground font-medium">Database:</span>
                                              <p className="font-mono text-foreground text-xs break-all">{conn.initialCatalog}</p>
                                            </div>
                                          )}
                                          {conn.provider && (
                                            <div>
                                              <span className="text-muted-foreground font-medium">Provider:</span>
                                              <p className="font-mono text-foreground text-xs break-all">{conn.provider}</p>
                                            </div>
                                          )}
                                        </div>
                                      )}
                                    </div>
                                  ))}
                                </div>
                              </CardContent>
                            </Card>
                          )}

                          {/* Activity Timeline */}
                          <Card className="mb-6">
                            <CardHeader>
                              <CardTitle>Workflow Timeline</CardTitle>
                              <CardDescription>
                                Visual representation of activity execution flow based on precedence constraints. Click on any activity to view details.
                              </CardDescription>
                            </CardHeader>
                            <CardContent>
                              <WorkflowTimeline
                                activities={parsedData.activities}
                                executionSequence={parsedData.executionSequence}
                                containers={parsedData.containers}
                                onActivitySelect={setSelectedActivity}
                              />
                            </CardContent>
                          </Card>
                        </div>
                      ) : (
                        <ActivityDetailView 
                          activity={selectedActivity} 
                          onBack={() => setSelectedActivity(null)} 
                        />
                      )}
                    </TabsContent>
                    
                    <TabsContent value="fabric" className="mt-0">
                      <FabricMappingView
                        parsedData={parsedData}
                        mappingTrace={mappingTrace}
                        fabricPipeline={fabricPipeline}
                        conversionSummary={conversionSummary}
                        isMapping={isMapping}
                        isGenerating={isGenerating}
                        onMapToFabric={handleMapToFabric}
                        onGeneratePipeline={handleGeneratePipeline}
                        onDownloadPipeline={handleDownloadPipeline}
                      />
                    </TabsContent>
                    
                    <TabsContent value="xml" className="mt-0">
                      <Card>
                        <CardHeader>
                          <CardTitle className="flex items-center gap-2">
                            <FileCode className="w-5 h-5" />
                            XML Content
                          </CardTitle>
                          <CardDescription>
                            Raw XML representation of the uploaded {selectedFile?.name.endsWith('.dtsx') ? 'DTSX' : 'XML'} file
                          </CardDescription>
                        </CardHeader>
                        <CardContent>
                          <div className="space-y-4">
                            {xmlContent && xmlContent.includes('<EncryptedData') && (
                              <div className="p-3 bg-yellow-500/10 border border-yellow-500/20 rounded-md">
                                <div className="flex items-center gap-2">
                                  <Lock className="w-4 h-4 text-yellow-600 dark:text-yellow-400" />
                                  <p className="text-xs text-muted-foreground">
                                    This package contains encrypted sections. The XML structure is shown below (encrypted data appears as <code className="text-xs bg-muted px-1 rounded">&lt;EncryptedData&gt;</code> elements).
                                  </p>
                                </div>
                              </div>
                            )}
                            <ScrollArea className="h-[calc(100vh-16rem)] w-full rounded-md border border-border bg-muted/30 p-4">
                              <pre className="text-xs font-mono text-foreground whitespace-pre-wrap break-words">
                                {xmlContent || 'No XML content available'}
                              </pre>
                            </ScrollArea>
                          </div>
                        </CardContent>
                      </Card>
                    </TabsContent>

                    <TabsContent value="unity-catalog" className="mt-0">
                      <div className="space-y-6">
                        {/* Metadata table SQL for ADF */}
                        <Card>
                          <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                              <Database className="w-5 h-5" />
                              Metadata Table SQL (ControlTableIntegrated)
                            </CardTitle>
                            <CardDescription>
                              INSERT script for dbo.ControlTableIntegrated. ADF metadata-driven pipeline reads this table to run copy activities.
                            </CardDescription>
                            <div className="flex justify-end">
                              <Button
                                variant="outline"
                                size="sm"
                                onClick={handleDownloadSql}
                                disabled={!unityCatalogSql || isLoadingUnityCatalog}
                                className="gap-2"
                              >
                                <FileDown className="w-4 h-4" />
                                Download .sql
                              </Button>
                              <Button
                                variant="default"
                                size="sm"
                                onClick={handleDownloadMigrationPackage}
                                disabled={!packageId}
                                className="gap-2 ml-2"
                              >
                                <Download className="w-4 h-4" />
                                Download Migration Package
                              </Button>
                            </div>
                          </CardHeader>
                          <CardContent>
                            {isLoadingUnityCatalog ? (
                              <div className="flex items-center gap-2 text-muted-foreground py-8">
                                <Loader2 className="w-5 h-5 animate-spin" />
                                <span>Generating SQL and notebooks…</span>
                              </div>
                            ) : (
                              <ScrollArea className="h-[320px] w-full rounded-md border border-border bg-muted/30 p-4">
                                <pre className="text-xs font-mono text-foreground whitespace-pre-wrap break-words">
                                  {unityCatalogSql ?? "Switch to this tab after parsing a package to generate the SQL."}
                                </pre>
                              </ScrollArea>
                            )}
                          </CardContent>
                        </Card>

                        {/* PySpark notebooks for transformations */}
                        <Card>
                          <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                              <Code className="w-5 h-5" />
                              PySpark Notebooks (Databricks Silver/Gold)
                            </CardTitle>
                            <CardDescription>
                              Downloadable PySpark notebooks for all Data Flow transformations in the SSIS package.
                            </CardDescription>
                          </CardHeader>
                          <CardContent>
                            {isLoadingUnityCatalog ? null : unityCatalogNotebooks && Object.keys(unityCatalogNotebooks).length > 0 ? (
                              <ul className="space-y-2">
                                {Object.entries(unityCatalogNotebooks).map(([filename, content]) => (
                                  <li key={filename} className="flex items-center justify-between gap-4 rounded-md border border-border bg-muted/20 p-3">
                                    <span className="text-sm font-mono truncate">{filename}</span>
                                    <Button
                                      variant="outline"
                                      size="sm"
                                      onClick={() => handleDownloadNotebook(filename, content)}
                                      className="gap-2 shrink-0"
                                    >
                                      <Download className="w-4 h-4" />
                                      Download
                                    </Button>
                                  </li>
                                ))}
                              </ul>
                            ) : (
                              <p className="text-sm text-muted-foreground py-4">
                                {parsedData ? "No Data Flow tasks found, or notebooks are still loading." : "Parse a package first to generate PySpark notebooks."}
                              </p>
                            )}
                          </CardContent>
                        </Card>

                        {/* Databricks CLI commands for pipelines and jobs */}
                        <Card>
                          <CardHeader>
                            <CardTitle className="flex items-center gap-2">
                              <Code className="w-5 h-5" />
                              Databricks CLI Commands
                            </CardTitle>
                            <CardDescription>
                              Copy-paste ready commands for each pipeline and job. Run from the extracted migration package root.
                            </CardDescription>
                          </CardHeader>
                          <CardContent>
                            {isLoadingUnityCatalog ? (
                              <div className="flex items-center gap-2 text-muted-foreground py-4">
                                <Loader2 className="w-5 h-5 animate-spin" />
                                <span>Loading CLI commands…</span>
                              </div>
                            ) : cliCommands && cliCommands.length > 0 ? (
                              <div className="space-y-4">
                                {cliCommands.map((cmd) => (
                                  <div
                                    key={cmd.table_key}
                                    className="rounded-md border border-border bg-muted/20 p-4 space-y-2"
                                  >
                                    <p className="text-xs font-medium text-muted-foreground uppercase tracking-wide">
                                      {cmd.pipeline_file}
                                    </p>
                                    <div className="flex items-center gap-2">
                                      <code className="flex-1 text-sm font-mono bg-background px-2 py-1.5 rounded border break-all">
                                        {cmd.pipeline_cmd}
                                      </code>
                                      <Button
                                        variant="ghost"
                                        size="icon"
                                        onClick={() => handleCopyCliCommand(cmd.pipeline_cmd)}
                                        className="shrink-0"
                                        title="Copy pipeline command"
                                      >
                                        <Copy className="w-4 h-4" />
                                      </Button>
                                    </div>
                                    <div className="flex items-center gap-2">
                                      <code className="flex-1 text-sm font-mono bg-background px-2 py-1.5 rounded border break-all">
                                        {cmd.job_cmd}
                                      </code>
                                      <Button
                                        variant="ghost"
                                        size="icon"
                                        onClick={() => handleCopyCliCommand(cmd.job_cmd)}
                                        className="shrink-0"
                                        title="Copy job command"
                                      >
                                        <Copy className="w-4 h-4" />
                                      </Button>
                                    </div>
                                  </div>
                                ))}
                                <p className="text-xs text-muted-foreground">
                                  Extract the migration package ZIP and run these commands from the package root directory.
                                </p>
                              </div>
                            ) : (
                              <p className="text-sm text-muted-foreground py-4">
                                {parsedData ? "No pipeline/job artifacts detected for this package." : "Parse a package first to generate CLI commands."}
                              </p>
                            )}
                          </CardContent>
                        </Card>
                      </div>
                    </TabsContent>
                  </Tabs>
                )}
              </div>
            </ScrollArea>
          </main>
        </div>
      </div>
      
    </>
  );
}

// Flow node: activity or container with inner activities
type FlowNode =
  | { type: "activity"; activity: Activity }
  | { type: "container"; activity: Activity; children: FlowNode[] };

// Workflow Timeline – flow by previous/next and execution order, with inner activities
function WorkflowTimeline({
  activities,
  executionSequence,
  containers,
  onActivitySelect,
}: {
  activities: Activity[];
  executionSequence?: Array<{ refId: string; name: string; type: string; order: number }>;
  containers?: Array<{ containerRefId: string; containerName: string; activityRefIds: string[] }>;
  onActivitySelect: (activity: Activity) => void;
}) {
  const activityMap = new Map<string, Activity>();
  activities.forEach((a) => {
    activityMap.set(a.id.trim(), a);
    activityMap.set(a.id, a);
  });

  // Ordered list from execution sequence (correct flow)
  let orderedActivities: Activity[] = [];
  if (executionSequence?.length) {
    executionSequence.forEach((seq) => {
      const refId = (seq.refId || "").trim();
      const act = activityMap.get(refId) ?? activities.find((a) => a.id.trim() === refId);
      if (act) orderedActivities.push(act);
    });
  }
  if (orderedActivities.length === 0) {
    const visited = new Set<string>();
    const queue = activities.filter((a) => !(a as any).previousActivities?.length) as Activity[];
    queue.forEach((a) => visited.add(a.id));
    while (queue.length) {
      const activity = queue.shift()!;
      orderedActivities.push(activity);
      (activity as any).nextActivities?.forEach((next: { id: string }) => {
        const nextAct = activityMap.get((next?.id ?? "").trim()) ?? activityMap.get(next?.id ?? "");
        if (nextAct && !visited.has(nextAct.id)) {
          const prevs = (nextAct as any).previousActivities ?? [];
          if (prevs.every((p: { id: string }) => visited.has((p?.id ?? "").trim()))) {
            visited.add(nextAct.id);
            queue.push(nextAct);
          }
        }
      });
    }
    activities.forEach((a) => {
      if (!visited.has(a.id)) orderedActivities.push(a);
    });
  }

  // Build hierarchy: containers with inner activities
  const buildFlowNodes = (): FlowNode[] => {
    const nodes: FlowNode[] = [];
    const stack: { activity: Activity; children: FlowNode[] }[] = [];

    orderedActivities.forEach((act) => {
      const parentRefId = ((act as any).parentContainerRefId as string)?.trim() ?? "";

      while (stack.length > 0 && stack[stack.length - 1].activity.id.trim() !== parentRefId) {
        stack.pop();
      }

      if ((act.type || "").includes("Sequence Container")) {
        const node: FlowNode = { type: "container", activity: act, children: [] };
        if (stack.length > 0) {
          stack[stack.length - 1].children.push(node);
        } else {
          nodes.push(node);
        }
        stack.push({ activity: act, children: (node as { type: "container"; activity: Activity; children: FlowNode[] }).children });
      } else {
        const node: FlowNode = { type: "activity", activity: act };
        if (stack.length > 0 && parentRefId) {
          stack[stack.length - 1].children.push(node);
        } else {
          nodes.push(node);
        }
      }
    });
    return nodes;
  };

  const flowNodes = buildFlowNodes();

  const getActivityIcon = (type: string) => {
    if (type?.includes('Data Flow') || type?.includes('Pipeline')) return <GitBranch className="w-4 h-4" />;
    if (type?.includes('SQL')) return <Database className="w-4 h-4" />;
    if (type?.includes('Script')) return <Code className="w-4 h-4" />;
    if (type?.includes('Sequence')) return <PlayCircle className="w-4 h-4" />;
    if (type?.includes('Execute Package') || type?.includes('PackageTask')) return <Layers className="w-4 h-4" />;
    return <PlayCircle className="w-4 h-4" />;
  };

  const getActivityTypeName = (type: string) => {
    if (type?.includes('Data Flow')) return 'Data Flow';
    if (type?.includes('SQL')) return 'SQL Task';
    if (type?.includes('Script')) return 'Script Task';
    if (type?.includes('Sequence Container')) return 'Sequence Container';
    if (type?.includes('Execute Package') || type?.includes('PackageTask')) return 'Execute Package Task';
    return type || 'Task';
  };

  const renderActivityCard = (activity: Activity, isNested: boolean) => (
    <button
      key={activity.id}
      onClick={() => onActivitySelect(activity)}
      className={`group relative p-4 rounded-lg border-2 border-border bg-card hover:border-primary hover:bg-primary/5 transition-all text-left min-w-[200px] max-w-[250px] ${isNested ? "ml-8 border-l-4 border-l-primary/50" : ""}`}
    >
      <div className="flex items-start gap-3">
        <div className="flex-shrink-0 mt-0.5 text-primary">{getActivityIcon(activity.type)}</div>
        <div className="flex-1 min-w-0">
          <p className="text-sm font-semibold text-foreground mb-1 group-hover:text-primary transition-colors truncate">
            {activity.name}
          </p>
          <Badge variant="secondary" className="text-xs mb-2">
            {getActivityTypeName(activity.type)}
          </Badge>
          {isNested && (
            <p className="text-xs text-muted-foreground">Inner activity</p>
          )}
          {activity.description && (
            <p className="text-xs text-muted-foreground line-clamp-2">{activity.description}</p>
          )}
        </div>
      </div>
    </button>
  );

  const renderNode = (node: FlowNode, showArrow: boolean) => (
    <div key={node.type === "container" ? node.activity.id : node.activity.id} className="flex flex-wrap items-center gap-4">
      {node.type === "container" ? (
        <div className="w-full">
          <button
            onClick={() => onActivitySelect(node.activity)}
            className="group relative p-4 rounded-lg border-2 border-dashed border-primary/50 bg-primary/5 hover:bg-primary/10 transition-all text-left min-w-[220px]"
          >
            <div className="flex items-start gap-3">
              <div className="flex-shrink-0 mt-0.5 text-primary">
                <PlayCircle className="w-4 h-4" />
              </div>
              <div className="flex-1 min-w-0">
                <p className="text-sm font-semibold text-foreground mb-1">{node.activity.name}</p>
                <Badge variant="outline" className="text-xs">Sequence Container</Badge>
              </div>
            </div>
          </button>
          <div className="ml-8 pl-4 border-l-2 border-border mt-2 space-y-2">
            {node.children.map((child, childIndex) => (
              <div key={child.type === "container" ? child.activity.id : child.activity.id} className="flex flex-wrap items-center gap-4">
                {child.type === "container" ? (
                  renderNode(child, childIndex < node.children.length - 1)
                ) : (
                  <>
                    {renderActivityCard(child.activity, true)}
                    {childIndex < node.children.length - 1 && (
                      <ArrowRight className="w-5 h-5 text-muted-foreground flex-shrink-0" />
                    )}
                  </>
                )}
              </div>
            ))}
          </div>
        </div>
      ) : (
        <>
          {renderActivityCard(node.activity, false)}
          {showArrow && <ArrowRight className="w-5 h-5 text-muted-foreground flex-shrink-0" />}
        </>
      )}
    </div>
  );

  return (
    <div className="space-y-6">
      <p className="text-xs text-muted-foreground mb-4">
        Flow order follows precedence constraints (previous → next). Sequence containers show their inner activities below.
      </p>
      {flowNodes.length > 0 ? (
        <div className="flex flex-col gap-6">
          {flowNodes.map((node, index) => (
            <div key={node.type === "container" ? node.activity.id : node.activity.id} className="flex flex-wrap items-start gap-4">
              <div className="flex items-center justify-center w-8 h-8 rounded-full bg-primary/10 text-primary font-semibold text-sm flex-shrink-0">
                {index + 1}
              </div>
              {renderNode(node, index < flowNodes.length - 1)}
            </div>
          ))}
        </div>
      ) : (
        <div className="text-center py-8 text-muted-foreground">
          <p>No workflow timeline available. Activities may not have precedence constraints defined.</p>
        </div>
      )}
    </div>
  );
}

// Activity Detail View Component
function ActivityDetailView({ activity, onBack }: { activity: Activity; onBack: () => void }) {
  return (
    <div>
      {/* Back Button */}
      <div className="mb-4">
        <Button
          variant="ghost"
          size="sm"
          onClick={onBack}
          className="gap-2"
        >
          <ArrowLeft className="w-4 h-4" />
          Back to Overview
        </Button>
      </div>

      {/* Activity Header */}
      <div className="mb-6">
        <div className="flex items-center gap-3 mb-2">
          <div className="w-12 h-12 rounded-lg bg-primary/10 flex items-center justify-center">
            {activity.type.includes('Pipeline') ? (
              <GitBranch className="w-6 h-6 text-primary" />
            ) : activity.type.includes('SQL') ? (
              <Database className="w-6 h-6 text-primary" />
            ) : activity.type.includes('Script') ? (
              <Code className="w-6 h-6 text-primary" />
            ) : (
              <PlayCircle className="w-6 h-6 text-primary" />
            )}
          </div>
          <div>
            <h2 className="text-2xl font-semibold text-foreground">{activity.name}</h2>
            <div className="flex items-center gap-2 mt-1">
              <Badge variant="secondary">
                {activity.type.includes('Pipeline') ? 'Data Flow Task' :
                 activity.type.includes('SQL') ? 'Execute SQL Task' :
                 activity.type.includes('Script') ? 'Script Task' :
                 activity.type.split('.').pop() || activity.type}
              </Badge>
              {activity.disabled && (
                <Badge variant="destructive">Disabled</Badge>
              )}
            </div>
          </div>
        </div>
        {activity.description && (
          <p className="text-sm text-muted-foreground">{activity.description}</p>
        )}
      </div>

      <Accordion 
        type="multiple" 
        defaultValue={
          activity.type === 'Execute SQL Task' && activity.sqlTaskProperties
            ? ['overview', 'connection-props', 'sql-command-props']
            : activity.components && activity.components.length > 0 && activity.type.includes('Pipeline')
            ? ['overview', 'dft-overview', 'components']
            : ['overview', 'properties']
        } 
        className="space-y-4"
      >
        {/* Overview Section */}
        <AccordionItem value="overview" className="border border-border rounded-lg px-4 bg-card">
          <AccordionTrigger className="hover:no-underline py-4">
            <span className="text-sm font-semibold text-foreground">Overview</span>
          </AccordionTrigger>
          <AccordionContent className="pb-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-1">
                  Activity ID
                </p>
                <p className="text-sm font-mono text-foreground">{activity.id}</p>
              </div>
              <div>
                <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-1">
                  Executable Type
                </p>
                <p className="text-sm font-mono text-foreground">{activity.executableType}</p>
              </div>
              {/* Previous Activities */}
              {activity.previousActivities && activity.previousActivities.length > 0 && (
                <div className="col-span-2">
                  <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2">
                    Previous Activities
                  </p>
                  <div className="space-y-2">
                    {activity.previousActivities.map((prevActivity, idx) => (
                      <div key={idx} className="p-2 bg-muted/50 rounded-md border border-border">
                        <div className="grid grid-cols-[100px_1fr] gap-2 text-xs">
                          <span className="text-muted-foreground font-medium">Name:</span>
                          <span className="font-semibold text-foreground">{prevActivity.name}</span>
                        </div>
                        <div className="grid grid-cols-[100px_1fr] gap-2 text-xs mt-1">
                          <span className="text-muted-foreground font-medium">ID:</span>
                          <span className="font-mono text-foreground">{prevActivity.id}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
              {(!activity.previousActivities || activity.previousActivities.length === 0) && (
                <div className="col-span-2">
                  <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-1">
                    Previous Activities
                  </p>
                  <p className="text-sm text-muted-foreground">None (This is a starting activity)</p>
                </div>
              )}
              {/* Next Activities */}
              {activity.nextActivities && activity.nextActivities.length > 0 && (
                <div className="col-span-2">
                  <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2">
                    Next Activities
                  </p>
                  <div className="space-y-2">
                    {activity.nextActivities.map((nextActivity, idx) => (
                      <div key={idx} className="p-2 bg-muted/50 rounded-md border border-border">
                        <div className="grid grid-cols-[100px_1fr] gap-2 text-xs">
                          <span className="text-muted-foreground font-medium">Name:</span>
                          <span className="font-semibold text-foreground">{nextActivity.name}</span>
                        </div>
                        <div className="grid grid-cols-[100px_1fr] gap-2 text-xs mt-1">
                          <span className="text-muted-foreground font-medium">ID:</span>
                          <span className="font-mono text-foreground">{nextActivity.id}</span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
              {(!activity.nextActivities || activity.nextActivities.length === 0) && (
                <div className="col-span-2">
                  <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-1">
                    Next Activities
                  </p>
                  <p className="text-sm text-muted-foreground">None (This is an ending activity)</p>
                </div>
              )}
              {/* Show connection details in Overview only for non-SQL and non-Data Flow tasks (SQL tasks have dedicated Connection Properties section, Data Flow tasks have Data Flow Overview section) */}
              {activity.connectionDetails && activity.type !== 'Execute SQL Task' && !(activity.components && activity.components.length > 0) && (
                <div className="col-span-2">
                  <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2">
                    Connection Details
                  </p>
                  <div className="space-y-2 p-3 bg-muted/50 rounded-md border border-border">
                    <div className="grid grid-cols-[120px_1fr] gap-2 text-xs">
                      <span className="text-muted-foreground font-medium">Name:</span>
                      <span className="font-mono text-foreground">{activity.connectionDetails.name}</span>
                    </div>
                    {activity.connectionDetails.dataSource && (
                      <div className="grid grid-cols-[120px_1fr] gap-2 text-xs">
                        <span className="text-muted-foreground font-medium">Data Source:</span>
                        <span className="font-mono text-foreground">{activity.connectionDetails.dataSource}</span>
                      </div>
                    )}
                    {activity.connectionDetails.initialCatalog && (
                      <div className="grid grid-cols-[120px_1fr] gap-2 text-xs">
                        <span className="text-muted-foreground font-medium">Database:</span>
                        <span className="font-mono text-foreground">{activity.connectionDetails.initialCatalog}</span>
                      </div>
                    )}
                    {activity.connectionDetails.userId && (
                      <div className="grid grid-cols-[120px_1fr] gap-2 text-xs">
                        <span className="text-muted-foreground font-medium">User ID:</span>
                        <span className="font-mono text-foreground">{activity.connectionDetails.userId}</span>
                      </div>
                    )}
                    {activity.connectionDetails.provider && (
                      <div className="grid grid-cols-[120px_1fr] gap-2 text-xs">
                        <span className="text-muted-foreground font-medium">Provider:</span>
                        <span className="font-mono text-foreground">{activity.connectionDetails.provider}</span>
                      </div>
                    )}
                    {activity.connectionDetails.connectionString && (
                      <div className="mt-2 pt-2 border-t border-border">
                        <p className="text-xs font-medium text-muted-foreground mb-1">Connection String:</p>
                        <p className="text-xs font-mono text-foreground break-all bg-background p-2 rounded border">
                          {activity.connectionDetails.connectionString}
                        </p>
                      </div>
                    )}
                  </div>
                </div>
              )}
              {/* Show connection ID only if no connection details and not an Execute SQL Task and not a Data Flow Task */}
              {activity.connectionId && !activity.connectionDetails && activity.type !== 'Execute SQL Task' && !(activity.components && activity.components.length > 0) && (
                <div className="col-span-2">
                  <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-1">
                    Connection ID
                  </p>
                  <p className="text-sm font-mono text-foreground">{activity.connectionId}</p>
                  <p className="text-xs text-muted-foreground mt-1">(Connection details not found)</p>
                </div>
              )}

              {/* Execute Package Task Properties - Nested inside Overview */}
              {activity.type === 'Execute Package Task' && activity.executePackageTaskProperties && (
                <div className="col-span-2 mt-6 pt-6 border-t border-border">
                  <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground mb-4">
                    📦 Execute Package Task Properties
                  </p>
                  <div className="space-y-4">
                    {/* Package Name */}
                    {activity.executePackageTaskProperties.packageName && (
                      <div className="p-4 bg-muted/50 rounded-md border border-border">
                        <div className="flex items-center gap-2 mb-3">
                          <FileText className="w-4 h-4 text-primary" />
                          <p className="text-sm font-semibold text-foreground">
                            Package Name
                          </p>
                        </div>
                        <p className="text-sm font-mono text-foreground break-all">
                          {activity.executePackageTaskProperties.packageName}
                        </p>
                      </div>
                    )}

                    {/* Use Project Reference */}
                    {activity.executePackageTaskProperties.useProjectReference !== undefined && (
                      <div className="grid grid-cols-[200px_1fr] gap-4 py-2 border-b border-border">
                        <div>
                          <p className="text-xs font-medium text-muted-foreground">Use Project Reference</p>
                          <p className="text-xs text-muted-foreground/70 mt-0.5">Reference from SSIS project</p>
                        </div>
                        <div>
                          <Badge variant={activity.executePackageTaskProperties.useProjectReference ? "default" : "secondary"} className="text-xs">
                            {activity.executePackageTaskProperties.useProjectReference ? "Yes" : "No"}
                          </Badge>
                        </div>
                      </div>
                    )}

                    {/* Parameter Assignments */}
                    {activity.executePackageTaskProperties.parameterAssignments && 
                     activity.executePackageTaskProperties.parameterAssignments.length > 0 && (
                      <div>
                        <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2">
                          Parameter Assignments ({activity.executePackageTaskProperties.parameterAssignments.length})
                        </p>
                        <div className="overflow-x-auto">
                          <table className="w-full text-xs">
                            <thead>
                              <tr className="border-b border-border">
                                <th className="text-left py-2 px-2 font-medium text-muted-foreground">Parameter Name</th>
                                <th className="text-left py-2 px-2 font-medium text-muted-foreground">Binded Variable/Parameter</th>
                              </tr>
                            </thead>
                            <tbody>
                              {activity.executePackageTaskProperties.parameterAssignments.map((param, idx) => (
                                <tr key={idx} className="border-b border-border/50 last:border-0">
                                  <td className="py-2 px-2 font-mono text-foreground">{param.parameterName || '-'}</td>
                                  <td className="py-2 px-2 font-mono text-foreground">{param.bindedVariableOrParameterName || '-'}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          </AccordionContent>
        </AccordionItem>

        {/* Properties Section */}
        {activity.properties && activity.properties.length > 0 && (
          <AccordionItem value="properties" className="border border-border rounded-lg px-4 bg-card">
            <AccordionTrigger className="hover:no-underline py-4">
              <span className="text-sm font-semibold text-foreground">
                Properties ({activity.properties.length})
              </span>
            </AccordionTrigger>
            <AccordionContent className="pb-4">
              <div className="space-y-3">
                {activity.properties.map((prop, idx) => (
                  <div key={idx} className="grid grid-cols-[200px_1fr] gap-4 py-2 border-b border-border last:border-0">
                    <div>
                      <p className="text-xs font-medium text-muted-foreground">
                        {prop.name}
                      </p>
                      {prop.description && (
                        <p className="text-xs text-muted-foreground/70 mt-0.5">
                          {prop.description}
                        </p>
                      )}
                    </div>
                    <div>
                      <p className="text-sm font-mono text-foreground break-all">
                        {String(prop.value)}
                      </p>
                      {prop.dataType && (
                        <p className="text-xs text-muted-foreground mt-0.5">
                          Type: {prop.dataType}
                        </p>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </AccordionContent>
          </AccordionItem>
        )}

        {/* SQL Command Section (Legacy - for backward compatibility) - Only show for non-Data Flow Tasks */}
        {activity.sqlCommand && !activity.sqlTaskProperties && !(activity.components && activity.components.length > 0) && (
          <AccordionItem value="sql" className="border border-border rounded-lg px-4 bg-card">
            <AccordionTrigger className="hover:no-underline py-4">
              <span className="text-sm font-semibold text-foreground">SQL Command</span>
            </AccordionTrigger>
            <AccordionContent className="pb-4">
              <pre className="bg-muted p-4 rounded-md text-xs font-mono text-foreground overflow-x-auto">
                {activity.sqlCommand}
              </pre>
            </AccordionContent>
          </AccordionItem>
        )}

        {/* Execute SQL Task Properties Section */}
        {activity.type === 'Execute SQL Task' && activity.sqlTaskProperties && (
          <>
            {/* Connection Properties */}
            <AccordionItem value="connection-props" className="border border-border rounded-lg px-4 bg-card">
              <AccordionTrigger className="hover:no-underline py-4">
                <span className="text-sm font-semibold text-foreground">
                  🔌 Connection Properties
                </span>
              </AccordionTrigger>
              <AccordionContent className="pb-4">
                <div className="space-y-4">
                  {/* Connection Details */}
                  {activity.connectionDetails ? (
                    <div className="p-4 bg-muted/50 rounded-md border border-border">
                      <div className="flex items-center gap-2 mb-3">
                        <Database className="w-4 h-4 text-primary" />
                        <p className="text-sm font-semibold text-foreground">
                          {activity.connectionDetails.name}
                        </p>
                        <Badge variant="outline" className="text-xs">
                          {activity.connectionDetails.creationName}
                        </Badge>
                      </div>
                      <div className="grid grid-cols-2 gap-3 text-xs">
                        {activity.connectionDetails.dataSource && (
                          <div>
                            <span className="font-medium uppercase tracking-wide text-muted-foreground">
                              Data Source:
                            </span>
                            <p className="font-mono text-foreground mt-0.5">{activity.connectionDetails.dataSource}</p>
                          </div>
                        )}
                        {activity.connectionDetails.initialCatalog && (
                          <div>
                            <span className="font-medium uppercase tracking-wide text-muted-foreground">
                              Database:
                            </span>
                            <p className="font-mono text-foreground mt-0.5">{activity.connectionDetails.initialCatalog}</p>
                          </div>
                        )}
                        {activity.connectionDetails.provider && (
                          <div>
                            <span className="font-medium uppercase tracking-wide text-muted-foreground">
                              Provider:
                            </span>
                            <p className="font-mono text-foreground mt-0.5">{activity.connectionDetails.provider}</p>
                          </div>
                        )}
                        {activity.connectionDetails.userId && (
                          <div>
                            <span className="font-medium uppercase tracking-wide text-muted-foreground">
                              User ID:
                            </span>
                            <p className="font-mono text-foreground mt-0.5">{activity.connectionDetails.userId}</p>
                          </div>
                        )}
                      </div>
                      {activity.connectionDetails.connectionString && (
                        <div className="mt-3 pt-3 border-t border-border">
                          <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2">
                            Connection String:
                          </p>
                          <p className="text-xs font-mono text-foreground break-all bg-background p-2 rounded border">
                            {activity.connectionDetails.connectionString}
                          </p>
                        </div>
                      )}
                    </div>
                  ) : (
                    <div className="p-4 bg-muted/30 rounded-md border border-border">
                      <p className="text-xs text-muted-foreground mb-2">Connection details not available</p>
                      {activity.sqlTaskProperties.connection && (
                        <div className="grid grid-cols-[120px_1fr] gap-2 text-xs">
                          <span className="text-muted-foreground font-medium">Connection ID:</span>
                          <span className="font-mono text-foreground break-all">
                            {activity.sqlTaskProperties.connection}
                          </span>
                        </div>
                      )}
                      {activity.connectionId && (
                        <div className="grid grid-cols-[120px_1fr] gap-2 text-xs mt-1">
                          <span className="text-muted-foreground font-medium">Connection ID:</span>
                          <span className="font-mono text-foreground break-all">
                            {activity.connectionId}
                          </span>
                        </div>
                      )}
                    </div>
                  )}

                  {/* Connection Manager ID (Reference) */}
                  {activity.sqlTaskProperties.connection && (
                    <div className="grid grid-cols-[200px_1fr] gap-4 py-2 border-b border-border">
                      <div>
                        <p className="text-xs font-medium text-muted-foreground">Connection Manager ID</p>
                        <p className="text-xs text-muted-foreground/70 mt-0.5">Internal GUID reference</p>
                      </div>
                      <div>
                        <p className="text-sm font-mono text-foreground break-all">
                          {activity.sqlTaskProperties.connection}
                        </p>
                      </div>
                    </div>
                  )}

                  {/* Timeout */}
                  {activity.sqlTaskProperties.timeout !== undefined && (
                    <div className="grid grid-cols-[200px_1fr] gap-4 py-2 border-b border-border last:border-0">
                      <div>
                        <p className="text-xs font-medium text-muted-foreground">Timeout (seconds)</p>
                        <p className="text-xs text-muted-foreground/70 mt-0.5">Max execution time (0 = no timeout)</p>
                      </div>
                      <div>
                        <p className="text-sm font-mono text-foreground">
                          {activity.sqlTaskProperties.timeout}
                        </p>
                      </div>
                    </div>
                  )}
                </div>
              </AccordionContent>
            </AccordionItem>

            {/* SQL Command Properties - Only show for Execute SQL Tasks, not Data Flow Tasks */}
            {activity.sqlTaskProperties && !(activity.components && activity.components.length > 0) && (
              <AccordionItem value="sql-command-props" className="border border-border rounded-lg px-4 bg-card">
                <AccordionTrigger className="hover:no-underline py-4">
                  <span className="text-sm font-semibold text-foreground">
                    📝 SQL Command Properties
                  </span>
                </AccordionTrigger>
                <AccordionContent className="pb-4">
                  <div className="space-y-3">
                    {activity.sqlTaskProperties.sqlStatementSource && (
                      <div className="mb-4">
                        <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2">
                          SQL Statement Source
                        </p>
                        <pre className="bg-muted p-4 rounded-md text-xs font-mono text-foreground overflow-x-auto whitespace-pre-wrap">
                          {activity.sqlTaskProperties.sqlStatementSource}
                        </pre>
                        
                        {/* Referenced Tables */}
                        {activity.sqlTaskProperties.referencedTables && activity.sqlTaskProperties.referencedTables.length > 0 && (
                          <div className="mt-3 pt-3 border-t border-border">
                            <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2">
                              📊 Referenced Tables
                            </p>
                            <div className="flex flex-wrap gap-2">
                              {activity.sqlTaskProperties.referencedTables.map((tableRef, idx) => (
                                <Badge key={idx} variant="secondary" className="text-xs font-mono break-all">
                                  {tableRef.fullName}
                                </Badge>
                              ))}
                            </div>
                          </div>
                        )}
                        {activity.sqlTaskProperties.referencedTables && activity.sqlTaskProperties.referencedTables.length === 0 && (
                          <div className="mt-3 pt-3 border-t border-border">
                            <p className="text-xs text-muted-foreground italic">No table references detected.</p>
                          </div>
                        )}
                      </div>
                    )}
                    {activity.sqlTaskProperties.sqlStatementSourceType && (
                      <div className="grid grid-cols-[200px_1fr] gap-4 py-2 border-b border-border">
                        <div>
                          <p className="text-xs font-medium text-muted-foreground">Source Type</p>
                          <p className="text-xs text-muted-foreground/70 mt-0.5">How SQL is provided</p>
                        </div>
                        <div>
                          <Badge variant="outline" className="text-xs">
                            {activity.sqlTaskProperties.sqlStatementSourceType === 'DirectInput' ? 'Direct Input' :
                             activity.sqlTaskProperties.sqlStatementSourceType === 'FileConnection' ? 'File Connection' :
                             activity.sqlTaskProperties.sqlStatementSourceType === 'Variable' ? 'Variable' :
                             activity.sqlTaskProperties.sqlStatementSourceType}
                          </Badge>
                        </div>
                      </div>
                    )}
                    {activity.sqlTaskProperties.isStoredProcedure !== undefined && (
                      <div className="grid grid-cols-[200px_1fr] gap-4 py-2 border-b border-border">
                        <div>
                          <p className="text-xs font-medium text-muted-foreground">Is Stored Procedure</p>
                        </div>
                        <div>
                          <Badge variant={activity.sqlTaskProperties.isStoredProcedure ? "default" : "secondary"} className="text-xs">
                            {activity.sqlTaskProperties.isStoredProcedure ? "Yes" : "No"}
                          </Badge>
                        </div>
                      </div>
                    )}
                    {activity.sqlTaskProperties.codePage && (
                      <div className="grid grid-cols-[200px_1fr] gap-4 py-2 border-b border-border last:border-0">
                        <div>
                          <p className="text-xs font-medium text-muted-foreground">Code Page</p>
                          <p className="text-xs text-muted-foreground/70 mt-0.5">Character encoding</p>
                        </div>
                        <div>
                          <p className="text-sm font-mono text-foreground">
                            {activity.sqlTaskProperties.codePage}
                          </p>
                        </div>
                      </div>
                    )}
                  </div>
                </AccordionContent>
              </AccordionItem>
            )}

            {/* Result Set Properties */}
            {(activity.sqlTaskProperties.resultSetType || activity.sqlTaskProperties.resultBindings) && (
              <AccordionItem value="resultset-props" className="border border-border rounded-lg px-4 bg-card">
                <AccordionTrigger className="hover:no-underline py-4">
                  <span className="text-sm font-semibold text-foreground">
                    📊 Result Set Properties
                  </span>
                </AccordionTrigger>
                <AccordionContent className="pb-4">
                  <div className="space-y-3">
                    {activity.sqlTaskProperties.resultSetType && (
                      <div className="grid grid-cols-[200px_1fr] gap-4 py-2 border-b border-border">
                        <div>
                          <p className="text-xs font-medium text-muted-foreground">Result Set Type</p>
                          <p className="text-xs text-muted-foreground/70 mt-0.5">Expected result format</p>
                        </div>
                        <div>
                          <Badge variant="outline" className="text-xs">
                            {activity.sqlTaskProperties.resultSetType === 'None' ? 'None (No results)' :
                             activity.sqlTaskProperties.resultSetType === 'SingleRow' ? 'Single Row' :
                             activity.sqlTaskProperties.resultSetType === 'Full' ? 'Full Result Set' :
                             activity.sqlTaskProperties.resultSetType === 'XML' ? 'XML Result' :
                             activity.sqlTaskProperties.resultSetType}
                          </Badge>
                        </div>
                      </div>
                    )}
                    {activity.sqlTaskProperties.resultBindings && activity.sqlTaskProperties.resultBindings.length > 0 && (
                      <div>
                        <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2">
                          Result Bindings ({activity.sqlTaskProperties.resultBindings.length})
                        </p>
                        <div className="overflow-x-auto">
                          <table className="w-full text-xs">
                            <thead>
                              <tr className="border-b border-border">
                                <th className="text-left py-2 px-2 font-medium text-muted-foreground">Result Name</th>
                                <th className="text-left py-2 px-2 font-medium text-muted-foreground">Variable Name</th>
                              </tr>
                            </thead>
                            <tbody>
                              {activity.sqlTaskProperties.resultBindings.map((binding, idx) => (
                                <tr key={idx} className="border-b border-border/50 last:border-0">
                                  <td className="py-2 px-2 font-mono text-foreground">{binding.resultName}</td>
                                  <td className="py-2 px-2 font-mono text-foreground">{binding.variableName || '-'}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}
                  </div>
                </AccordionContent>
              </AccordionItem>
            )}

            {/* Parameter Mapping Properties */}
            {activity.sqlTaskProperties.parameterBindings && activity.sqlTaskProperties.parameterBindings.length > 0 && (
              <AccordionItem value="parameter-props" className="border border-border rounded-lg px-4 bg-card">
                <AccordionTrigger className="hover:no-underline py-4">
                  <span className="text-sm font-semibold text-foreground">
                    🔗 Parameter Mapping ({activity.sqlTaskProperties.parameterBindings.length})
                  </span>
                </AccordionTrigger>
                <AccordionContent className="pb-4">
                  <div className="overflow-x-auto">
                    <table className="w-full text-xs">
                      <thead>
                        <tr className="border-b border-border">
                          <th className="text-left py-2 px-2 font-medium text-muted-foreground">Parameter Name</th>
                          <th className="text-left py-2 px-2 font-medium text-muted-foreground">Variable Name</th>
                          <th className="text-left py-2 px-2 font-medium text-muted-foreground">Direction</th>
                          <th className="text-left py-2 px-2 font-medium text-muted-foreground">Data Type</th>
                          <th className="text-left py-2 px-2 font-medium text-muted-foreground">Size</th>
                        </tr>
                      </thead>
                      <tbody>
                        {activity.sqlTaskProperties.parameterBindings.map((param, idx) => (
                          <tr key={idx} className="border-b border-border/50 last:border-0">
                            <td className="py-2 px-2 font-mono text-foreground">{param.name}</td>
                            <td className="py-2 px-2 font-mono text-foreground">{param.variableName || '-'}</td>
                            <td className="py-2 px-2">
                              <Badge variant="outline" className="text-xs">
                                {param.direction || '-'}
                              </Badge>
                            </td>
                            <td className="py-2 px-2 font-mono text-foreground">{param.dataType || '-'}</td>
                            <td className="py-2 px-2 font-mono text-foreground">{param.size || '-'}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </AccordionContent>
              </AccordionItem>
            )}

            {/* Additional SQL Task Properties */}
            {activity.sqlTaskProperties.bypassPrepare !== undefined && (
              <AccordionItem value="additional-props" className="border border-border rounded-lg px-4 bg-card">
                <AccordionTrigger className="hover:no-underline py-4">
                  <span className="text-sm font-semibold text-foreground">⚙️ Additional Properties</span>
                </AccordionTrigger>
                <AccordionContent className="pb-4">
                  <div className="space-y-3">
                    {activity.sqlTaskProperties.bypassPrepare !== undefined && (
                      <div className="grid grid-cols-[200px_1fr] gap-4 py-2 border-b border-border last:border-0">
                        <div>
                          <p className="text-xs font-medium text-muted-foreground">Bypass Prepare</p>
                          <p className="text-xs text-muted-foreground/70 mt-0.5">Skip prepared statement optimization</p>
                        </div>
                        <div>
                          <Badge variant={activity.sqlTaskProperties.bypassPrepare ? "default" : "secondary"} className="text-xs">
                            {activity.sqlTaskProperties.bypassPrepare ? "Yes" : "No"}
                          </Badge>
                        </div>
                      </div>
                    )}
                  </div>
                </AccordionContent>
              </AccordionItem>
            )}
          </>
        )}

        {/* Data Flow Task Overview */}
        {activity.components && activity.components.length > 0 && activity.type.includes('Pipeline') && (
          <AccordionItem value="dft-overview" className="border border-border rounded-lg px-4 bg-card">
            <AccordionTrigger className="hover:no-underline py-4">
              <span className="text-sm font-semibold text-foreground">📊 Data Flow Overview</span>
            </AccordionTrigger>
            <AccordionContent className="pb-4">
              <div className="grid grid-cols-2 gap-4 mb-4">
                <div>
                  <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-1">
                    Total Components
                  </p>
                  <p className="text-sm font-mono text-foreground">{activity.components.length}</p>
                </div>
                <div>
                  <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-1">
                    Component Types
                  </p>
                  <div className="flex flex-wrap gap-1 mt-1">
                    {Array.from(new Set(activity.components.map(c => c.componentType))).map(type => (
                      <Badge key={type} variant="outline" className="text-xs">
                        {type}
                      </Badge>
                    ))}
                  </div>
                </div>
              </div>
              
              {/* Source and Destination Connections */}
              <div className="grid grid-cols-2 gap-4">
                {/* Source Connection */}
                {activity.components.filter(c => c.componentType === 'Source').length > 0 && (
                  <div className="p-3 bg-muted/50 rounded-md border border-border">
                    <div className="flex items-center gap-2 mb-3">
                      <Database className="w-4 h-4 text-primary" />
                      <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                        Source Connection
                      </p>
                    </div>
                    {(() => {
                      const sourceComponent = activity.components.find(c => c.componentType === 'Source');
                      if (sourceComponent?.connectionDetails) {
                        return (
                          <div className="space-y-2">
                            <div className="grid grid-cols-[80px_1fr] gap-2 text-xs">
                              <span className="text-muted-foreground font-medium">Name:</span>
                              <span className="font-mono text-foreground">{sourceComponent.connectionDetails.name}</span>
                            </div>
                            {sourceComponent.connectionDetails.dataSource && (
                              <div className="grid grid-cols-[80px_1fr] gap-2 text-xs">
                                <span className="text-muted-foreground font-medium">Server:</span>
                                <span className="font-mono text-foreground">{sourceComponent.connectionDetails.dataSource}</span>
                              </div>
                            )}
                            {sourceComponent.connectionDetails.initialCatalog && (
                              <div className="grid grid-cols-[80px_1fr] gap-2 text-xs">
                                <span className="text-muted-foreground font-medium">Database:</span>
                                <span className="font-mono text-foreground">{sourceComponent.connectionDetails.initialCatalog}</span>
                              </div>
                            )}
                            {sourceComponent.connectionDetails.userId && (
                              <div className="grid grid-cols-[80px_1fr] gap-2 text-xs">
                                <span className="text-muted-foreground font-medium">User:</span>
                                <span className="font-mono text-foreground">{sourceComponent.connectionDetails.userId}</span>
                              </div>
                            )}
                          </div>
                        );
                      } else if (sourceComponent?.connectionId) {
                        return (
                          <div className="text-xs">
                            <p className="text-muted-foreground mb-1">Connection ID:</p>
                            <p className="font-mono text-foreground break-all">{sourceComponent.connectionId}</p>
                          </div>
                        );
                      } else {
                        return (
                          <p className="text-xs text-muted-foreground">No connection configured</p>
                        );
                      }
                    })()}
                  </div>
                )}

                {/* Destination Connection */}
                {activity.components.filter(c => c.componentType === 'Destination').length > 0 && (
                  <div className="p-3 bg-muted/50 rounded-md border border-border">
                    <div className="flex items-center gap-2 mb-3">
                      <Database className="w-4 h-4 text-primary" />
                      <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                        Destination Connection
                      </p>
                    </div>
                    {(() => {
                      const destComponent = activity.components.find(c => c.componentType === 'Destination');
                      if (destComponent?.connectionDetails) {
                        return (
                          <div className="space-y-2">
                            <div className="grid grid-cols-[80px_1fr] gap-2 text-xs">
                              <span className="text-muted-foreground font-medium">Name:</span>
                              <span className="font-mono text-foreground">{destComponent.connectionDetails.name}</span>
                            </div>
                            {destComponent.tableName && (
                              <div className="grid grid-cols-[80px_1fr] gap-2 text-xs">
                                <span className="text-muted-foreground font-medium">Table:</span>
                                <span className="font-mono text-foreground font-semibold text-primary">{destComponent.tableName}</span>
                              </div>
                            )}
                            {destComponent.connectionDetails.dataSource && (
                              <div className="grid grid-cols-[80px_1fr] gap-2 text-xs">
                                <span className="text-muted-foreground font-medium">Server:</span>
                                <span className="font-mono text-foreground">{destComponent.connectionDetails.dataSource}</span>
                              </div>
                            )}
                            {destComponent.connectionDetails.initialCatalog && (
                              <div className="grid grid-cols-[80px_1fr] gap-2 text-xs">
                                <span className="text-muted-foreground font-medium">Database:</span>
                                <span className="font-mono text-foreground">{destComponent.connectionDetails.initialCatalog}</span>
                              </div>
                            )}
                            {destComponent.connectionDetails.userId && (
                              <div className="grid grid-cols-[80px_1fr] gap-2 text-xs">
                                <span className="text-muted-foreground font-medium">User:</span>
                                <span className="font-mono text-foreground">{destComponent.connectionDetails.userId}</span>
                              </div>
                            )}
                          </div>
                        );
                      } else if (destComponent?.connectionId) {
                        return (
                          <div className="text-xs space-y-2">
                            {destComponent.tableName && (
                              <div className="grid grid-cols-[80px_1fr] gap-2">
                                <span className="text-muted-foreground font-medium">Table:</span>
                                <span className="font-mono text-foreground font-semibold text-primary">{destComponent.tableName}</span>
                              </div>
                            )}
                            <div>
                              <p className="text-muted-foreground mb-1">Connection ID:</p>
                              <p className="font-mono text-foreground break-all">{destComponent.connectionId}</p>
                            </div>
                          </div>
                        );
                      } else {
                        return (
                          <div className="text-xs">
                            {destComponent?.tableName && (
                              <div className="grid grid-cols-[80px_1fr] gap-2 mb-2">
                                <span className="text-muted-foreground font-medium">Table:</span>
                                <span className="font-mono text-foreground font-semibold text-primary">{destComponent.tableName}</span>
                              </div>
                            )}
                            <p className="text-muted-foreground">No connection configured</p>
                          </div>
                        );
                      }
                    })()}
                  </div>
                )}
              </div>
            </AccordionContent>
          </AccordionItem>
        )}

        {/* Data Flow Components Section */}
        {activity.components && activity.components.length > 0 && (
          <AccordionItem value="components" className="border border-border rounded-lg px-4 bg-card">
            <AccordionTrigger className="hover:no-underline py-4">
              <span className="text-sm font-semibold text-foreground">
                Data Flow Components ({activity.components.length})
              </span>
            </AccordionTrigger>
            <AccordionContent className="pb-4">
              <div className="space-y-4">
                {[...activity.components].sort((a, b) => {
                  // Sort: Source first, then Destination, then others (preserve original order for same type)
                  const order = { 'Source': 1, 'Destination': 2 };
                  const aOrder = order[a.componentType as keyof typeof order] || 3;
                  const bOrder = order[b.componentType as keyof typeof order] || 3;
                  if (aOrder !== bOrder) return aOrder - bOrder;
                  return 0;
                }).map((component, idx) => (
                  <div key={idx} className="border border-border rounded-md p-4 bg-muted/30">
                    <div className="flex items-center gap-2 mb-3">
                      <GitBranch className="w-4 h-4 text-primary" />
                      <p className="text-sm font-semibold text-foreground">
                        {component.name}
                      </p>
                      <Badge variant="outline" className="text-xs">
                        {component.componentType}
                      </Badge>
                    </div>
                    
                    {component.description && (
                      <p className="text-xs text-muted-foreground mb-3">
                        {component.description}
                      </p>
                    )}

                    {/* Connection Details for Source and Destination Components */}
                    {(component.componentType === 'Source' || component.componentType === 'Destination') && (
                      <div className="mb-3 p-3 bg-muted/50 rounded-md border border-border">
                        <div className="flex items-center gap-2 mb-2">
                          <Database className="w-4 h-4 text-primary" />
                          <p className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                            {component.componentType} Connection
                          </p>
                        </div>
                        {component.connectionDetails ? (
                          <div className="space-y-2">
                            <div className="grid grid-cols-[100px_1fr] gap-2 text-xs">
                              <span className="text-muted-foreground font-medium">Name:</span>
                              <span className="font-mono text-foreground">{component.connectionDetails.name}</span>
                            </div>
                            {component.tableName && component.componentType === 'Destination' && (
                              <div className="grid grid-cols-[100px_1fr] gap-2 text-xs">
                                <span className="text-muted-foreground font-medium">Table:</span>
                                <span className="font-mono text-foreground font-semibold text-primary">{component.tableName}</span>
                              </div>
                            )}
                            {component.connectionDetails.dataSource && (
                              <div className="grid grid-cols-[100px_1fr] gap-2 text-xs">
                                <span className="text-muted-foreground font-medium">Data Source:</span>
                                <span className="font-mono text-foreground">{component.connectionDetails.dataSource}</span>
                              </div>
                            )}
                            {component.connectionDetails.initialCatalog && (
                              <div className="grid grid-cols-[100px_1fr] gap-2 text-xs">
                                <span className="text-muted-foreground font-medium">Database:</span>
                                <span className="font-mono text-foreground">{component.connectionDetails.initialCatalog}</span>
                              </div>
                            )}
                            {component.connectionDetails.userId && (
                              <div className="grid grid-cols-[100px_1fr] gap-2 text-xs">
                                <span className="text-muted-foreground font-medium">User ID:</span>
                                <span className="font-mono text-foreground">{component.connectionDetails.userId}</span>
                              </div>
                            )}
                            {component.connectionDetails.provider && (
                              <div className="grid grid-cols-[100px_1fr] gap-2 text-xs">
                                <span className="text-muted-foreground font-medium">Provider:</span>
                                <span className="font-mono text-foreground">{component.connectionDetails.provider}</span>
                              </div>
                            )}
                            {component.connectionDetails.connectionString && (
                              <div className="mt-2 pt-2 border-t border-border">
                                <p className="text-xs font-medium text-muted-foreground mb-1">Connection String:</p>
                                <p className="text-xs font-mono text-foreground break-all bg-background p-2 rounded border">
                                  {component.connectionDetails.connectionString}
                                </p>
                              </div>
                            )}
                          </div>
                        ) : component.connectionId ? (
                          <div className="text-xs space-y-2">
                            {component.tableName && component.componentType === 'Destination' && (
                              <div className="grid grid-cols-[100px_1fr] gap-2">
                                <span className="text-muted-foreground font-medium">Table:</span>
                                <span className="font-mono text-foreground font-semibold text-primary">{component.tableName}</span>
                              </div>
                            )}
                            <div>
                              <p className="text-muted-foreground mb-1">Connection ID:</p>
                              <p className="font-mono text-foreground break-all">{component.connectionId}</p>
                              <p className="text-muted-foreground mt-1 text-xs">(Connection details not found)</p>
                            </div>
                          </div>
                        ) : (
                          <div className="text-xs">
                            {component.tableName && component.componentType === 'Destination' && (
                              <div className="grid grid-cols-[100px_1fr] gap-2 mb-2">
                                <span className="text-muted-foreground font-medium">Table:</span>
                                <span className="font-mono text-foreground font-semibold text-primary">{component.tableName}</span>
                              </div>
                            )}
                            <p className="text-muted-foreground">No connection configured</p>
                          </div>
                        )}
                      </div>
                    )}

                    {/* Component Connections (Multiple connections used in transformations) - Only for Transformation components, not Source/Destination */}
                    {component.componentType !== 'Source' && 
                     component.componentType !== 'Destination' && 
                     component.componentConnections && 
                     component.componentConnections.length > 0 && (
                      <div className="mb-3 p-3 bg-blue-50 dark:bg-blue-950 border border-blue-200 dark:border-blue-800 rounded-md">
                        <p className="text-xs font-medium uppercase tracking-wide text-blue-800 dark:text-blue-200 mb-2">
                          Connections Used in This Component
                        </p>
                        <div className="space-y-2">
                          {component.componentConnections.map((compConn, idx) => (
                            <div key={idx} className="text-xs bg-background dark:bg-slate-900 p-2 rounded border border-border">
                              <div className="font-medium text-foreground mb-1">{compConn.name || 'Connection'}</div>
                              {compConn.connectionManagerRefId && (
                                <div className="grid grid-cols-[80px_1fr] gap-2">
                                  <span className="text-muted-foreground">RefId:</span>
                                  <span className="font-mono text-foreground break-all text-xs">{compConn.connectionManagerRefId}</span>
                                </div>
                              )}
                              {compConn.description && (
                                <div className="mt-1">
                                  <span className="text-muted-foreground text-xs italic">{compConn.description}</span>
                                </div>
                              )}
                            </div>
                          ))}
                        </div>
                      </div>
                    )}

                    {/* Source Query for Source Components (includes queries resolved from SqlCommandVariable) */}
                    {component.componentType === 'Source' && component.sourceMetadata?.sourceQuery && (
                      <div className="mb-3 p-3 bg-blue-50 dark:bg-blue-950 border border-blue-200 dark:border-blue-800 rounded-md">
                        <p className="text-xs font-semibold uppercase tracking-wide text-blue-900 dark:text-blue-100 mb-2">
                          📋 Source Query
                          {component.sourceMetadata.sqlCommandVariableRef && (
                            <span className="ml-2 text-xs font-normal normal-case text-blue-700 dark:text-blue-300">
                              (from variable: {component.sourceMetadata.sqlCommandVariableRef})
                            </span>
                          )}
                        </p>
                        <pre className="text-xs font-mono text-foreground whitespace-pre-wrap break-words bg-background dark:bg-slate-900 p-3 rounded border border-border max-h-60 overflow-y-auto">
                          {component.sourceMetadata.sourceQuery}
                        </pre>
                      </div>
                    )}

                    {/* Referenced Tables for Source Components */}
                    {component.componentType === 'Source' && component.sourceMetadata?.referencedTables && component.sourceMetadata.referencedTables.length > 0 && (
                      <div className="mb-3 p-3 bg-blue-50 dark:bg-blue-950 border border-blue-200 dark:border-blue-800 rounded-md">
                        <p className="text-xs font-semibold uppercase tracking-wide text-blue-900 dark:text-blue-100 mb-2">
                          📊 Referenced Tables ({component.sourceMetadata.referencedTables.length})
                        </p>
                        <div className="flex flex-wrap gap-2">
                          {component.sourceMetadata.referencedTables.map((tableRef, idx) => (
                            <Badge key={idx} variant="secondary" className="text-xs font-mono break-all cursor-help" title={`${tableRef.database ? tableRef.database + '.' : ''}${tableRef.schema ? tableRef.schema + '.' : ''}${tableRef.table}`}>
                              {tableRef.fullName}
                            </Badge>
                          ))}
                        </div>
                      </div>
                    )}

                    {/* Referenced Tables for Destination Components */}
                    {component.componentType === 'Destination' && component.destinationMetadata?.referencedTables && component.destinationMetadata.referencedTables.length > 0 && (
                      <div className="mb-3 p-3 bg-green-50 dark:bg-green-950 border border-green-200 dark:border-green-800 rounded-md">
                        <p className="text-xs font-semibold uppercase tracking-wide text-green-900 dark:text-green-100 mb-2">
                          📊 Referenced Tables ({component.destinationMetadata.referencedTables.length})
                        </p>
                        <div className="flex flex-wrap gap-2">
                          {component.destinationMetadata.referencedTables.map((tableRef, idx) => (
                            <Badge key={idx} variant="secondary" className="text-xs font-mono break-all cursor-help" title={`${tableRef.database ? tableRef.database + '.' : ''}${tableRef.schema ? tableRef.schema + '.' : ''}${tableRef.table}`}>
                              {tableRef.fullName}
                            </Badge>
                          ))}
                        </div>
                      </div>
                    )}

                    {/* Referenced Tables for Transformations (Lookup SQL, etc.) */}
                    {component.componentType === 'Transformation' && component.transformationLogic?.referencedTables && component.transformationLogic.referencedTables.length > 0 && (
                      <div className="mb-3 p-3 bg-amber-50 dark:bg-amber-950 border border-amber-200 dark:border-amber-800 rounded-md">
                        <p className="text-xs font-semibold uppercase tracking-wide text-amber-900 dark:text-amber-100 mb-2">
                          📊 Referenced Tables ({component.transformationLogic.referencedTables.length})
                        </p>
                        <p className="text-xs text-muted-foreground mb-2">
                          From SQL in this transformation (e.g. Lookup command).
                        </p>
                        <div className="flex flex-wrap gap-2">
                          {component.transformationLogic.referencedTables.map((tableRef, idx) => (
                            <Badge key={idx} variant="secondary" className="text-xs font-mono break-all cursor-help" title={tableRef.fullName}>
                              {tableRef.fullName}
                            </Badge>
                          ))}
                        </div>
                      </div>
                    )}

                    {/* Component Properties */}
                    {component.properties.length > 0 && (
                      <div className="mb-3">
                        <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2">
                          Component Properties
                        </p>
                        <div className="space-y-2">
                          {component.properties.slice(0, 5).map((prop, propIdx) => (
                            <div key={propIdx} className="flex justify-between text-xs">
                              <span className="text-muted-foreground">{prop.name}:</span>
                              <span className="font-mono text-foreground">{String(prop.value)}</span>
                            </div>
                          ))}
                        </div>
                      </div>
                    )}

                    {/* Input Columns */}
                    {component.inputColumns.length > 0 && (
                      <div className="mb-3">
                        <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2">
                          Input Columns ({component.inputColumns.length})
                        </p>
                        <div className="overflow-x-auto">
                          <table className="w-full text-xs">
                            <thead>
                              <tr className="border-b border-border">
                                <th className="text-left py-2 px-2 font-medium text-muted-foreground">Name</th>
                                <th className="text-left py-2 px-2 font-medium text-muted-foreground">Data Type</th>
                                <th className="text-left py-2 px-2 font-medium text-muted-foreground">Length</th>
                              </tr>
                            </thead>
                            <tbody>
                              {component.inputColumns.map((col, colIdx) => (
                                <tr key={colIdx} className="border-b border-border/50 last:border-0">
                                  <td className="py-2 px-2 font-mono text-foreground">{col.name}</td>
                                  <td className="py-2 px-2 font-mono text-foreground">{col.dataType}</td>
                                  <td className="py-2 px-2 font-mono text-foreground">
                                    {col.length || col.precision || '-'}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}

                    {/* Output Columns */}
                    {component.outputColumns.length > 0 && (
                      <div>
                        <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-2">
                          Output Columns ({component.outputColumns.length})
                        </p>
                        <div className="overflow-x-auto">
                          <table className="w-full text-xs">
                            <thead>
                              <tr className="border-b border-border">
                                <th className="text-left py-2 px-2 font-medium text-muted-foreground">Name</th>
                                <th className="text-left py-2 px-2 font-medium text-muted-foreground">Data Type</th>
                                <th className="text-left py-2 px-2 font-medium text-muted-foreground">Length</th>
                              </tr>
                            </thead>
                            <tbody>
                              {component.outputColumns.map((col, colIdx) => (
                                <tr key={colIdx} className="border-b border-border/50 last:border-0">
                                  <td className="py-2 px-2 font-mono text-foreground">{col.name}</td>
                                  <td className="py-2 px-2 font-mono text-foreground">{col.dataType}</td>
                                  <td className="py-2 px-2 font-mono text-foreground">
                                    {col.length || col.precision || '-'}
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            </AccordionContent>
          </AccordionItem>
        )}
      </Accordion>
    </div>
  );
}

// Fabric Mapping View Component
function FabricMappingView({
  parsedData,
  mappingTrace,
  fabricPipeline,
  conversionSummary,
  isMapping,
  isGenerating,
  onMapToFabric,
  onGeneratePipeline,
  onDownloadPipeline,
}: {
  parsedData: ParsedPackage | null;
  mappingTrace: MappingTrace | null;
  fabricPipeline: any;
  conversionSummary: ConversionSummary | null;
  isMapping: boolean;
  isGenerating: boolean;
  onMapToFabric: () => void;
  onGeneratePipeline: () => void;
  onDownloadPipeline: () => void;
}) {
  if (!parsedData) {
    return (
      <div className="flex flex-col items-center justify-center h-[calc(100vh-8rem)] text-center">
        <Cloud className="w-16 h-16 text-muted-foreground mb-4" />
        <h2 className="text-xl font-semibold text-foreground mb-2">
          Parse SSIS Package First
        </h2>
        <p className="text-sm text-muted-foreground max-w-md">
          Please parse an SSIS package file before mapping to Fabric.
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Header Actions */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-semibold text-foreground mb-2">
            SSIS → Fabric Pipeline Conversion
          </h2>
          <p className="text-sm text-muted-foreground">
            Map your SSIS package to Microsoft Fabric Pipeline format
          </p>
        </div>
        <div className="flex gap-2">
          {!mappingTrace && (
            <Button onClick={onMapToFabric} disabled={isMapping}>
              {isMapping ? (
                <>
                  <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                  Mapping...
                </>
              ) : (
                <>
                  <Cloud className="w-4 h-4 mr-2" />
                  Map to Fabric
                </>
              )}
            </Button>
          )}
          {mappingTrace && !fabricPipeline && (
            <Button onClick={onGeneratePipeline} disabled={isGenerating}>
              {isGenerating ? (
                <>
                  <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                  Generating...
                </>
              ) : (
                <>
                  <GitBranch className="w-4 h-4 mr-2" />
                  Generate Pipeline
                </>
              )}
            </Button>
          )}
          {fabricPipeline && (
            <Button onClick={onDownloadPipeline}>
              <Download className="w-4 h-4 mr-2" />
              Download JSON
            </Button>
          )}
        </div>
      </div>

      {/* Conversion Summary */}
      {conversionSummary && (
        <Card>
          <CardHeader>
            <CardTitle>Conversion Summary</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-4 gap-4 mb-4">
              <div>
                <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-1">
                  Overall Confidence
                </p>
                <p className="text-2xl font-semibold text-foreground">
                  {(conversionSummary.overallConfidence * 100).toFixed(0)}%
                </p>
              </div>
              <div>
                <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-1">
                  Fully Supported
                </p>
                <p className="text-2xl font-semibold text-green-600">
                  {conversionSummary.supportBreakdown.fullySupported}
                </p>
              </div>
              <div>
                <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-1">
                  Partially Supported
                </p>
                <p className="text-2xl font-semibold text-yellow-600">
                  {conversionSummary.supportBreakdown.partiallySupported}
                </p>
              </div>
              <div>
                <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground mb-1">
                  Requires Manual Review
                </p>
                <p className="text-2xl font-semibold text-red-600">
                  {conversionSummary.manualRemediationCount}
                </p>
              </div>
            </div>
            {conversionSummary.validation && (
              <div className="mt-4">
                {conversionSummary.validation.valid ? (
                  <Alert>
                    <CheckCircle2 className="w-4 h-4" />
                    <AlertTitle>Pipeline Valid</AlertTitle>
                    <AlertDescription>
                      The generated Fabric pipeline is valid and ready for deployment.
                    </AlertDescription>
                  </Alert>
                ) : (
                  <Alert variant="destructive">
                    <XCircle className="w-4 h-4" />
                    <AlertTitle>Validation Errors</AlertTitle>
                    <AlertDescription>
                      <ul className="list-disc list-inside mt-2">
                        {conversionSummary.validation.errors.map((error, idx) => (
                          <li key={idx} className="text-sm">{error}</li>
                        ))}
                      </ul>
                    </AlertDescription>
                  </Alert>
                )}
              </div>
            )}
          </CardContent>
        </Card>
      )}

      {/* Mapping Trace - Side by Side View */}
      {mappingTrace && (
        <Card>
          <CardHeader>
            <CardTitle>Activity Mapping</CardTitle>
            <CardDescription>
              SSIS activities mapped to Fabric activities
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {mappingTrace.mappedActivities.map((mapped, idx) => (
                <div key={idx} className="border border-border rounded-lg p-4 bg-muted/30">
                  <div className="grid grid-cols-2 gap-4">
                    {/* SSIS Side */}
                    <div>
                      <div className="flex items-center gap-2 mb-2">
                        <FileText className="w-4 h-4 text-primary" />
                        <p className="text-sm font-semibold text-foreground">SSIS Activity</p>
                      </div>
                      <div className="space-y-2">
                        <div>
                          <p className="text-xs text-muted-foreground">Name</p>
                          <p className="text-sm font-mono text-foreground">{mapped.ssis.name}</p>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground">Type</p>
                          <Badge variant="secondary" className="text-xs">
                            {mapped.ssis.type}
                          </Badge>
                        </div>
                      </div>
                    </div>
                    
                    {/* Fabric Side */}
                    <div>
                      <div className="flex items-center gap-2 mb-2">
                        <Cloud className="w-4 h-4 text-primary" />
                        <p className="text-sm font-semibold text-foreground">Fabric Activity</p>
                      </div>
                      <div className="space-y-2">
                        <div>
                          <p className="text-xs text-muted-foreground">Type</p>
                          <Badge 
                            variant={
                              mapped.classification.classification === '✅ Fully supported' 
                                ? 'default' 
                                : mapped.classification.classification === '⚠ Partially supported'
                                ? 'secondary'
                                : 'destructive'
                            }
                            className="text-xs"
                          >
                            {mapped.fabric.activityType || 'Unsupported'}
                          </Badge>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground">Status</p>
                          <Badge variant="outline" className="text-xs">
                            {mapped.classification.classification}
                          </Badge>
                        </div>
                        <div>
                          <p className="text-xs text-muted-foreground">Confidence</p>
                          <p className="text-sm font-mono text-foreground">
                            {(mapped.classification.confidenceScore * 100).toFixed(0)}%
                          </p>
                        </div>
                      </div>
                    </div>
                  </div>
                  
                  {/* Warnings */}
                  {mapped.classification.warnings && mapped.classification.warnings.length > 0 && (
                    <div className="mt-4 pt-4 border-t border-border">
                      <div className="flex items-start gap-2">
                        <AlertTriangle className="w-4 h-4 text-yellow-600 mt-0.5" />
                        <div>
                          <p className="text-xs font-semibold text-foreground mb-1">Warnings</p>
                          <ul className="list-disc list-inside space-y-1">
                            {mapped.classification.warnings.map((warning, wIdx) => (
                              <li key={wIdx} className="text-xs text-muted-foreground">
                                {warning}
                              </li>
                            ))}
                          </ul>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Manual Remediation List */}
      {mappingTrace && mappingTrace.manualRemediationList.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle>Manual Remediation Required</CardTitle>
            <CardDescription>
              {mappingTrace.manualRemediationList.length} activities require manual intervention
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {mappingTrace.manualRemediationList.map((item, idx) => (
                <div key={idx} className="border border-yellow-500/20 bg-yellow-500/10 rounded-md p-3">
                  <div className="flex items-center gap-2 mb-2">
                    <AlertTriangle className="w-4 h-4 text-yellow-600" />
                    <p className="text-sm font-semibold text-foreground">{item.activityName}</p>
                    <Badge variant="outline" className="text-xs">{item.activityType}</Badge>
                  </div>
                  <div>
                    <p className="text-xs text-muted-foreground mb-1">Reasons:</p>
                    <ul className="list-disc list-inside space-y-1">
                      {item.reason.map((reason, rIdx) => (
                        <li key={rIdx} className="text-xs text-muted-foreground">
                          {reason}
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Pipeline Preview */}
      {fabricPipeline && (
        <Card>
          <CardHeader>
            <CardTitle>Fabric Pipeline Preview</CardTitle>
            <CardDescription>
              Generated pipeline: {fabricPipeline.name}
            </CardDescription>
          </CardHeader>
          <CardContent>
            <ScrollArea className="h-[400px] w-full rounded-md border border-border bg-muted/30 p-4">
              <pre className="text-xs font-mono text-foreground whitespace-pre-wrap break-words">
                {JSON.stringify(fabricPipeline, null, 2)}
              </pre>
            </ScrollArea>
          </CardContent>
        </Card>
      )}

      {/* Instructions */}
      {!mappingTrace && (
        <Card>
          <CardHeader>
            <CardTitle>Getting Started</CardTitle>
          </CardHeader>
          <CardContent>
            <ol className="list-decimal list-inside space-y-2 text-sm text-muted-foreground">
              <li>Click "Map to Fabric" to analyze your SSIS package and generate mapping trace</li>
              <li>Review the mapping results and check for warnings or unsupported features</li>
              <li>Click "Generate Pipeline" to create the Fabric Pipeline JSON</li>
              <li>Download the generated pipeline JSON for deployment to Microsoft Fabric</li>
            </ol>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
