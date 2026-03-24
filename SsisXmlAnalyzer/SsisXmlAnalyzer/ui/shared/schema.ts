import { z } from "zod";

// SSIS Package Metadata
export const packageMetadataSchema = z.object({
  name: z.string(),
  objectName: z.string(),
  creationDate: z.string(),
  creator: z.string(),
  creatorComputer: z.string(),
  dtsId: z.string(),
  versionBuild: z.string().optional(),
  versionGuid: z.string().optional(),
  description: z.string().optional(),
});

export type PackageMetadata = z.infer<typeof packageMetadataSchema>;

// Connection Manager
export const connectionManagerSchema = z.object({
  id: z.string(),
  name: z.string(),
  creationName: z.string(),
  dtsId: z.string().optional(),
  connectionString: z.string().optional(),
  provider: z.string().optional(),
  dataSource: z.string().optional(),
  initialCatalog: z.string().optional(),
  userId: z.string().optional(),
});

export type ConnectionManager = z.infer<typeof connectionManagerSchema>;

// Property
export const propertySchema = z.object({
  name: z.string(),
  value: z.union([z.string(), z.number(), z.boolean()]),
  dataType: z.string().optional(),
  description: z.string().optional(),
});

export type Property = z.infer<typeof propertySchema>;

// Column Mapping
export const columnMappingSchema = z.object({
  name: z.string(),
  dataType: z.string(),
  length: z.number().optional(),
  precision: z.number().optional(),
  scale: z.number().optional(),
  sourceColumn: z.string().optional(),
  lineageId: z.string().optional(),
});

export type ColumnMapping = z.infer<typeof columnMappingSchema>;

// Table Reference (extracted from SQL queries)
export const tableReferenceSchema = z.object({
  database: z.string().optional(),
  schema: z.string().optional(),
  table: z.string(),
  fullName: z.string(),
});

export type TableReference = z.infer<typeof tableReferenceSchema>;

// Component Connection (for transformation connections)
export const componentConnectionSchema = z.object({
  connectionManagerRefId: z.string().optional(),
  connectionManagerID: z.string().optional(),
  name: z.string().optional(),
  description: z.string().optional(),
  refId: z.string().optional(),
});

export type ComponentConnection = z.infer<typeof componentConnectionSchema>;

// Source Metadata (for Source components)
export const sourceMetadataSchema = z.object({
  sourceID: z.number().optional(),
  sourceSchemaName: z.string().optional(),
  sourceTableName: z.string().optional(),
  sourceQuery: z.string().optional(),
  openRowset: z.string().optional(),
  referencedTables: z.array(tableReferenceSchema).optional(), // Tables referenced in the query
  sqlCommandVariableRef: z.string().optional(), // e.g. "User::Qry" when query comes from variable
  sqlCommandSourceInfo: z.record(z.unknown()).optional(), // Metadata about variable resolution
});

export type SourceMetadata = z.infer<typeof sourceMetadataSchema>;

// Destination Metadata (for Destination components)
export const destinationMetadataSchema = z.object({
  targetSchemaName: z.string().optional(),
  targetTableName: z.string().optional(),
  targetDBName: z.string().optional(),
  copyMode: z.string().optional(), // Full or Incremental
  openRowset: z.string().optional(),
  sqlCommand: z.string().optional(),
  referencedTables: z.array(tableReferenceSchema).optional(), // Tables referenced in the query
});

export type DestinationMetadata = z.infer<typeof destinationMetadataSchema>;

// Transformation Logic
export const transformationLogicSchema = z.object({
  pysparkEquivalent: z.string().optional(),
  columnMappings: z.array(z.object({
    outputColumn: z.string(),
    expression: z.string(),
    friendlyExpression: z.string(),
  })).optional(),
  expressions: z.array(z.object({
    outputColumn: z.string(),
    expression: z.string(),
    friendlyExpression: z.string(),
  })).optional(),
  referencedTables: z.array(tableReferenceSchema).optional(), // From Lookup SqlCommand, etc.
});

export type TransformationLogic = z.infer<typeof transformationLogicSchema>;

// Data Flow Component
export const dataFlowComponentSchema = z.object({
  id: z.string(),
  name: z.string(),
  componentType: z.string(),
  description: z.string().optional(),
  properties: z.array(propertySchema),
  inputColumns: z.array(columnMappingSchema),
  outputColumns: z.array(columnMappingSchema),
  connectionId: z.string().optional(),
  tableName: z.string().optional(),
  connectionDetails: connectionManagerSchema.optional(), // Resolved connection details
  componentConnections: z.array(componentConnectionSchema).optional(), // Multiple connections used in component
  sourceMetadata: sourceMetadataSchema.optional(),
  destinationMetadata: destinationMetadataSchema.optional(),
  transformationLogic: transformationLogicSchema.optional(),
  componentClassID: z.string().optional(),
  componentTypeName: z.string().optional(),
  requiresManualReview: z.boolean().optional(),
});

export type DataFlowComponent = z.infer<typeof dataFlowComponentSchema>;

// SQL Task Parameter Binding
export const sqlParameterBindingSchema = z.object({
  name: z.string(),
  direction: z.string().optional(),
  dataType: z.string().optional(),
  size: z.string().optional(),
  variableName: z.string().optional(),
});

export type SqlParameterBinding = z.infer<typeof sqlParameterBindingSchema>;

// SQL Task Result Binding
export const sqlResultBindingSchema = z.object({
  resultName: z.string(),
  variableName: z.string().optional(),
});

export type SqlResultBinding = z.infer<typeof sqlResultBindingSchema>;

// SQL Task Properties
export const sqlTaskPropertiesSchema = z.object({
  sqlStatementSource: z.string().optional(),
  sqlStatementSourceType: z.string().optional(), // DirectInput, FileConnection, Variable
  connection: z.string().optional(),
  timeout: z.string().optional(),
  codePage: z.string().optional(),
  bypassPrepare: z.boolean().optional(),
  resultSetType: z.string().optional(), // None, SingleRow, Full, XML
  isStoredProcedure: z.boolean().optional(),
  parameterBindings: z.array(sqlParameterBindingSchema).optional(),
  resultBindings: z.array(sqlResultBindingSchema).optional(),
  referencedTables: z.array(tableReferenceSchema).optional(), // Tables referenced in the SQL query
});

export type SqlTaskProperties = z.infer<typeof sqlTaskPropertiesSchema>;

// Execute Package Task Parameter Assignment
export const executePackageParameterAssignmentSchema = z.object({
  parameterName: z.string().optional(),
  bindedVariableOrParameterName: z.string().optional(),
});

export type ExecutePackageParameterAssignment = z.infer<typeof executePackageParameterAssignmentSchema>;

// Execute Package Task Properties
export const executePackageTaskPropertiesSchema = z.object({
  packageName: z.string().optional(),
  useProjectReference: z.boolean().optional(),
  parameterAssignments: z.array(executePackageParameterAssignmentSchema).optional(),
});

export type ExecutePackageTaskProperties = z.infer<typeof executePackageTaskPropertiesSchema>;

// Activity/Executable
export const activitySchema = z.object({
  id: z.string(),
  name: z.string(),
  type: z.string(),
  executableType: z.string(),
  description: z.string().optional(),
  disabled: z.boolean().optional(),
  properties: z.array(propertySchema),
  sqlCommand: z.string().optional(), // For backward compatibility
  sqlTaskProperties: sqlTaskPropertiesSchema.optional(), // Enhanced SQL Task properties
  executePackageTaskProperties: executePackageTaskPropertiesSchema.optional(), // Execute Package Task properties
  components: z.array(dataFlowComponentSchema).optional(),
  precedenceConstraints: z.array(z.string()).optional(),
  previousActivities: z.array(z.object({
    id: z.string(),
    name: z.string(),
  })).optional(),
  nextActivities: z.array(z.object({
    id: z.string(),
    name: z.string(),
  })).optional(),
  connectionId: z.string().optional(),
  connectionDetails: connectionManagerSchema.optional(), // Resolved connection details
});

export type Activity = z.infer<typeof activitySchema>;

// Component Summary Item
export const componentSummaryItemSchema = z.object({
  name: z.string(),
  count: z.number(),
});

export type ComponentSummaryItem = z.infer<typeof componentSummaryItemSchema>;

// Component Summary
export const componentSummarySchema = z.object({
  activityTypeCounts: z.array(componentSummaryItemSchema),
  dataFlowComponentTypeCounts: z.array(componentSummaryItemSchema),
  totalActivities: z.number(),
  totalDataFlowComponents: z.number(),
}).optional();

export type ComponentSummary = z.infer<typeof componentSummarySchema>;

// Connection Usage Details
export const connectionUsageDetailSchema = z.object({
  usedIn: z.string(),
  activityName: z.string().optional(),
  activityType: z.string().optional(),
  activityId: z.string().optional(),
  componentName: z.string().optional(),
  componentType: z.string().optional(),
  componentId: z.string().optional(),
  connectionName: z.string().optional(),
  connectionDescription: z.string().optional(),
  locationInPackage: z.string(),
});

export type ConnectionUsageDetail = z.infer<typeof connectionUsageDetailSchema>;

// Connection with Usage Info
export const connectionWithUsageSchema = connectionManagerSchema.extend({
  usageCount: z.number(),
  usedInActivities: z.number(),
  usedInComponents: z.number(),
  usageDetails: z.array(connectionUsageDetailSchema),
});

export type ConnectionWithUsage = z.infer<typeof connectionWithUsageSchema>;

// Connections Usage Map
export const connectionsUsageMapSchema = z.object({
  allConnections: z.array(connectionWithUsageSchema),
  connectionUsageMap: z.record(z.string(), z.array(connectionUsageDetailSchema)),
  unusedConnections: z.array(connectionWithUsageSchema),
  totalConnections: z.number(),
  usedConnections: z.number(),
  unusedConnectionCount: z.number(),
});

export type ConnectionsUsageMap = z.infer<typeof connectionsUsageMapSchema>;

// Execution Sequence Item
export const executionSequenceItemSchema = z.object({
  refId: z.string(),
  name: z.string(),
  type: z.string(),
  order: z.number(),
});

export type ExecutionSequenceItem = z.infer<typeof executionSequenceItemSchema>;

// Precedence Constraint
export const precedenceConstraintSchema = z.object({
  to: z.string(),
  from: z.array(z.string()),
});

export type PrecedenceConstraint = z.infer<typeof precedenceConstraintSchema>;

// Constraint Detail
export const constraintDetailSchema = z.object({
  fromRefId: z.string(),
  toRefId: z.string(),
  value: z.string(),
  logicalAnd: z.boolean(),
  constraintName: z.string().optional(),
});

export type ConstraintDetail = z.infer<typeof constraintDetailSchema>;

// Activity Flow Graph
export const activityFlowGraphSchema = z.object({
  firstActivities: z.array(z.object({
    refId: z.string(),
    name: z.string(),
  })),
  lastActivities: z.array(z.object({
    refId: z.string(),
    name: z.string(),
  })),
  constraintsDetail: z.array(constraintDetailSchema),
  precedenceMap: z.record(z.string(), z.array(z.string())),
});

export type ActivityFlowGraph = z.infer<typeof activityFlowGraphSchema>;

// Container
export const containerSchema = z.object({
  containerRefId: z.string(),
  containerName: z.string(),
  activityRefIds: z.array(z.string()),
  activities: z.array(z.object({
    refId: z.string(),
    name: z.string(),
    type: z.string(),
  })).optional(),
});

export type Container = z.infer<typeof containerSchema>;

// Package-wide referenced tables (aggregated from all SQL in the package)
export const packageReferencedTableDetailSchema = z.object({
  table: tableReferenceSchema,
  referencedFrom: z.array(z.string()),
});

export type PackageReferencedTableDetail = z.infer<typeof packageReferencedTableDetailSchema>;

// Parsed SSIS Package
export const parsedPackageSchema = z.object({
  metadata: packageMetadataSchema,
  connectionManagers: z.array(connectionManagerSchema),
  connectionsUsageMap: connectionsUsageMapSchema.optional(),
  activities: z.array(activitySchema),
  variables: z.array(propertySchema).optional(),
  componentSummary: componentSummarySchema,
  executionSequence: z.array(executionSequenceItemSchema).optional(),
  precedenceConstraints: z.array(precedenceConstraintSchema).optional(),
  activityFlowGraph: activityFlowGraphSchema.optional(),
  containers: z.array(containerSchema).optional(),
  packageReferencedTables: z.array(tableReferenceSchema).optional(),
  packageReferencedTablesDetailed: z.array(packageReferencedTableDetailSchema).optional(),
});

export type ParsedPackage = z.infer<typeof parsedPackageSchema>;

// API Response for file upload
export const uploadResponseSchema = z.object({
  success: z.boolean(),
  message: z.string().optional(),
  data: parsedPackageSchema.optional(),
  packageId: z.string().optional(),
});

export type UploadResponse = z.infer<typeof uploadResponseSchema>;

// Fabric Mapping Schemas (Module 6, 7, 8)

// Mapping Result
export const mappingResultSchema = z.object({
  ssisActivityId: z.string().optional(),
  ssisActivityName: z.string().optional(),
  ssisActivityType: z.string().optional(),
  targetActivityType: z.string().optional(),
  confidence: z.number().optional(),
  supportLevel: z.string().optional(),
  warnings: z.array(z.string()).optional(),
  unsupported: z.array(z.string()).optional(),
  requiresManualIntervention: z.boolean().optional(),
  mapping: z.record(z.any()).optional(),
});

export type MappingResult = z.infer<typeof mappingResultSchema>;

// Classification
export const classificationSchema = z.object({
  componentId: z.string().optional(),
  componentName: z.string().optional(),
  componentType: z.string().optional(),
  classification: z.string(),
  confidenceScore: z.number(),
  targetType: z.string().optional(),
  warnings: z.array(z.string()).optional(),
  requiresManualIntervention: z.boolean(),
  explanation: z.string().optional(),
});

export type Classification = z.infer<typeof classificationSchema>;

// Mapping Trace
export const mappingTraceSchema = z.object({
  mappedActivities: z.array(z.object({
    ssis: z.object({
      id: z.string(),
      name: z.string(),
      type: z.string(),
    }),
    fabric: z.object({
      activityType: z.string().optional(),
      mapping: z.record(z.any()).optional(),
    }),
    classification: classificationSchema,
    mappingResult: mappingResultSchema,
  })),
  variablesMapping: z.object({
    targetActivityType: z.string().optional(),
    confidence: z.number().optional(),
    supportLevel: z.string().optional(),
    mapping: z.record(z.any()).optional(),
  }).optional(),
  diagnostics: z.object({
    totalActivities: z.number(),
    mappedActivities: z.number(),
    overallConfidence: z.number(),
    warnings: z.array(z.string()).optional(),
    manualRemediationCount: z.number(),
  }),
  manualRemediationList: z.array(z.object({
    activityId: z.string(),
    activityName: z.string(),
    activityType: z.string(),
    reason: z.array(z.string()),
  })),
  conversionSummary: z.object({
    fullySupported: z.number(),
    partiallySupported: z.number(),
    unsupported: z.number(),
  }),
});

export type MappingTrace = z.infer<typeof mappingTraceSchema>;

// Pipeline Validation
export const validationSchema = z.object({
  valid: z.boolean(),
  errors: z.array(z.string()),
  warnings: z.array(z.string()),
});

export type Validation = z.infer<typeof validationSchema>;

// Conversion Summary
export const conversionSummarySchema = z.object({
  conversionDate: z.string(),
  originalPackage: z.number(),
  convertedActivities: z.number(),
  overallConfidence: z.number(),
  validation: validationSchema,
  supportBreakdown: z.object({
    fullySupported: z.number(),
    partiallySupported: z.number(),
    unsupported: z.number(),
  }),
  manualRemediationCount: z.number(),
  warnings: z.array(z.string()).optional(),
  pipelineName: z.string(),
  pipelineParameters: z.number(),
});

export type ConversionSummary = z.infer<typeof conversionSummarySchema>;
