from fastapi import FastAPI, File, UploadFile, HTTPException, Body, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, FileResponse
from lxml import etree
from typing import List, Dict, Any, Optional, Union, Set, Tuple
import re
import subprocess
import tempfile
import os
import platform
import json
import sys
import uuid
import shutil
import zipfile
import time
from pathlib import Path

# Try to import sqlglot for SQL parsing
try:
    import sqlglot
    from sqlglot import parse_one, exp
    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False
    print("Warning: sqlglot not available. SQL table extraction will use fallback regex method.")

# Import mapping and pipeline generation modules
# Add the api directory to the path to ensure imports work
api_dir = Path(__file__).parent
if str(api_dir) not in sys.path:
    sys.path.insert(0, str(api_dir))

try:
    from fabric_mapping_engine import MappingEngine
    from fabric_pipeline_generator import FabricPipelineGenerator
    MAPPING_ENGINE_AVAILABLE = True
except ImportError as e:
    MappingEngine = None
    FabricPipelineGenerator = None
    MAPPING_ENGINE_AVAILABLE = False
    print(f"Warning: Fabric mapping modules not available: {e}")

app = FastAPI()

# In-memory store for already-parsed package metadata keyed by packageId.
# The generator endpoint ONLY consumes this parsed metadata (no re-parsing of DTSX).
_PARSED_PACKAGE_STORE: Dict[str, Dict[str, Any]] = {}
_PARSED_PACKAGE_STORE_MAX = 50

try:
    from migration_artifact_generator import MigrationArtifactGenerator
    MIGRATION_ARTIFACTS_AVAILABLE = True
except ImportError as e:
    MigrationArtifactGenerator = None
    MIGRATION_ARTIFACTS_AVAILABLE = False
    print(f"Warning: Migration artifact generator not available: {e}")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Namespace for SSIS DTSX files
DTS_NAMESPACE = {'DTS': 'www.microsoft.com/SqlServer/Dts'}


if SQLGLOT_AVAILABLE:
    def _sqlglot_ident_to_str(node: Any) -> str:
        """Normalize sqlglot Identifier / string nodes to plain text."""
        if node is None:
            return ""
        if isinstance(node, str):
            return node.strip()
        nm = getattr(node, "name", None)
        if isinstance(nm, str) and nm.strip():
            return nm.strip()
        this = getattr(node, "this", None)
        if this is not None:
            return str(this).strip()
        return str(node).strip()

    def _sqlglot_strip_brackets(s: str) -> str:
        s = s.strip()
        if len(s) >= 2 and s[0] == "[" and s[-1] == "]":
            return s[1:-1]
        return s

    def _sqlglot_collect_cte_names(expression: exp.Expression) -> Set[str]:
        """Names of CTEs in the query; FROM <cte> parses as exp.Table and must be skipped."""
        names: Set[str] = set()
        for cte in expression.find_all(exp.CTE):
            alias = cte.args.get("alias")
            if alias is None:
                continue
            if isinstance(alias, exp.TableAlias):
                s = _sqlglot_ident_to_str(alias.this)
            else:
                s = _sqlglot_ident_to_str(alias)
            if s:
                names.add(s.upper())
        return names

    def _sqlglot_is_cte_reference(table: exp.Table, cte_names: Set[str]) -> bool:
        """True when this Table is an unqualified reference to a CTE (not a same-named schema.table)."""
        n = _sqlglot_ident_to_str(table.name)
        if not n or n.upper() not in cte_names:
            return False
        if table.db or table.catalog:
            return False
        return True

    def _sqlglot_qualified_table_name(table: exp.Table) -> Tuple[str, str, str, str]:
        """Build (database, schema, table, fullName) from exp.Table only — excludes AS alias on the Table node."""
        catalog = ""
        if table.catalog:
            catalog = _sqlglot_strip_brackets(_sqlglot_ident_to_str(table.catalog))
        db = ""
        if table.db:
            db = _sqlglot_strip_brackets(_sqlglot_ident_to_str(table.db))
        name = _sqlglot_strip_brackets(_sqlglot_ident_to_str(table.name))
        if not name:
            return ("", "", "", "")
        parts = [p for p in (catalog, db, name) if p]
        full_name = ".".join(parts)
        return (catalog, db, name, full_name)

    def _sqlglot_extract_physical_tables(sql_query: str) -> Optional[List[Dict[str, str]]]:
        """
        Parse SQL with sqlglot and return only physical table references (exp.Table), deduplicated.
        Traverses nested subqueries, joins, and window queries via find_all(exp.Table).
        Returns None if no dialect could parse the statement; [] if parsed but no base tables.
        """
        parsed: Optional[exp.Expression] = None
        for dialect in ("tsql", "oracle", None):
            try:
                parsed = parse_one(sql_query, dialect=dialect)
                break
            except Exception:
                continue
        if parsed is None:
            return None

        cte_names = _sqlglot_collect_cte_names(parsed)
        seen: Set[str] = set()
        tables: List[Dict[str, str]] = []

        for table in parsed.find_all(exp.Table):
            if _sqlglot_is_cte_reference(table, cte_names):
                continue
            database_name, schema_name, table_name, full_name = _sqlglot_qualified_table_name(table)
            if not full_name:
                continue
            if table_name.upper() == "DUAL" and not schema_name and not database_name:
                continue
            key = full_name.upper()
            if key in seen:
                continue
            seen.add(key)
            tables.append({
                "database": database_name,
                "schema": schema_name,
                "table": table_name,
                "fullName": full_name,
            })
        return tables


def extract_table_references(sql_query: str) -> List[Dict[str, str]]:
    """
    Extract database.schema.table references from a SQL query.
    Uses sqlglot to collect only physical tables (exp.Table): not columns, functions, or aliases.
    Skips CTE names referenced as tables. Deduplicates by full qualified name.
    Falls back to regex only when sqlglot cannot parse the statement.
    """
    if not sql_query or not isinstance(sql_query, str):
        return []

    tables: List[Dict[str, str]] = []
    seen_tables: Set[str] = set()

    try:
        if SQLGLOT_AVAILABLE:
            try:
                sqlglot_tables = _sqlglot_extract_physical_tables(sql_query)
                if sqlglot_tables is not None:
                    return sqlglot_tables
            except Exception as e:
                print(f"sqlglot extraction failed: {e}. Using regex fallback.")

        # Fallback: Use regex-based parsing for common SQL patterns
        # Remove comments
        sql_clean = re.sub(r'--[^\n]*', '', sql_query, flags=re.MULTILINE)
        sql_clean = re.sub(r'/\*.*?\*/', '', sql_clean, flags=re.DOTALL)
        
        # Helper function to parse a table name/reference
        def parse_table_name(table_ref: str) -> tuple:
            """
            Parse a table reference and extract database, schema, table.
            Handles: [db].[schema].[table], db.schema.table, [schema].[table], schema.table, [table], table
            Returns: (database, schema, table)
            """
            table_ref = table_ref.strip()
            if not table_ref:
                return ("", "", "")
            
            # Remove surrounding brackets
            table_ref = table_ref.strip('[]')
            
            # Split by dots, but handle brackets first
            # First, check if it starts with brackets
            parts = []
            current_part = ""
            in_bracket = False
            
            for char in table_ref:
                if char == '[':
                    in_bracket = True
                elif char == ']':
                    in_bracket = False
                elif char == '.' and not in_bracket:
                    if current_part:
                        parts.append(current_part.strip('[]'))
                        current_part = ""
                    continue
                current_part += char
            
            if current_part:
                parts.append(current_part.strip('[]'))
            
            # Now we have the parts
            database = ""
            schema = ""
            table = ""
            
            if len(parts) == 3:
                database, schema, table = parts
            elif len(parts) == 2:
                schema, table = parts
            elif len(parts) == 1:
                table = parts[0]
            
            return (database, schema, table)
        
        # Pattern 1: FROM [database].[schema].[table] or FROM [schema].[table] or FROM table
        # This pattern captures the entire table reference up to a space or comment
        from_pattern = r'FROM\s+([^\s,;]+(?:\.\s*[^\s,;]+)*)'
        for match in re.finditer(from_pattern, sql_clean, re.IGNORECASE):
            table_ref = match.group(1).replace('\n', ' ').strip()
            if table_ref and not table_ref.lower().startswith('('):  # Skip subqueries
                db, schema, table = parse_table_name(table_ref)
                if table:
                    full_name = ".".join(filter(None, [db, schema, table]))
                    if full_name not in seen_tables:
                        seen_tables.add(full_name)
                        tables.append({
                            'database': db,
                            'schema': schema,
                            'table': table,
                            'fullName': full_name
                        })
        
        # Pattern 2: JOIN [database].[schema].[table]
        join_pattern = r'(?:INNER|LEFT|RIGHT|FULL|CROSS)?\s*JOIN\s+([^\s]+(?:\.\s*[^\s]+)*)'
        for match in re.finditer(join_pattern, sql_clean, re.IGNORECASE):
            table_ref = match.group(1).replace('\n', ' ').strip()
            if table_ref and not table_ref.lower().startswith('('):  # Skip subqueries
                db, schema, table = parse_table_name(table_ref)
                if table:
                    full_name = ".".join(filter(None, [db, schema, table]))
                    if full_name not in seen_tables:
                        seen_tables.add(full_name)
                        tables.append({
                            'database': db,
                            'schema': schema,
                            'table': table,
                            'fullName': full_name
                        })
        
        # Pattern 3: INSERT INTO, UPDATE, DELETE FROM
        insert_pattern = r'(?:INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+([^\s,;]+(?:\.\s*[^\s,;]+)*)'
        for match in re.finditer(insert_pattern, sql_clean, re.IGNORECASE):
            table_ref = match.group(1).replace('\n', ' ').strip()
            if table_ref and not table_ref.lower().startswith('('):  # Skip subqueries
                db, schema, table = parse_table_name(table_ref)
                if table:
                    full_name = ".".join(filter(None, [db, schema, table]))
                    if full_name not in seen_tables:
                        seen_tables.add(full_name)
                        tables.append({
                            'database': db,
                            'schema': schema,
                            'table': table,
                            'fullName': full_name
                        })
        
        # Pattern 4: INTO table (for INSERT INTO ... SELECT ... INTO)
        into_pattern = r'INTO\s+([^\s,;]+(?:\.\s*[^\s,;]+)*)'
        for match in re.finditer(into_pattern, sql_clean, re.IGNORECASE):
            table_ref = match.group(1).replace('\n', ' ').strip()
            if table_ref and not table_ref.lower().startswith('('):  # Skip subqueries
                db, schema, table = parse_table_name(table_ref)
                if table:
                    full_name = ".".join(filter(None, [db, schema, table]))
                    if full_name not in seen_tables:
                        seen_tables.add(full_name)
                        tables.append({
                            'database': db,
                            'schema': schema,
                            'table': table,
                            'fullName': full_name
                        })
        
        return tables
    
    except Exception as e:
        # If anything goes wrong, return empty list
        print(f"Error extracting table references: {e}")
        return []


def merge_referenced_tables_lists(*lists: List[Optional[List[Dict[str, str]]]]) -> List[Dict[str, str]]:
    """Merge multiple referenced-table lists, deduplicating by fullName (first occurrence wins)."""
    seen: Set[str] = set()
    out: List[Dict[str, str]] = []
    for lst in lists:
        for t in (lst or []):
            if not isinstance(t, dict):
                continue
            fn = (t.get('fullName') or '').strip()
            if not fn or fn in seen:
                continue
            seen.add(fn)
            out.append(t)
    return out


def _sql_text_usable_for_table_extraction(sql: Optional[str]) -> bool:
    """Skip dynamic placeholders and empty strings when extracting table references."""
    if not sql or not isinstance(sql, str):
        return False
    s = sql.strip()
    if s.startswith('[Dynamic query'):
        return False
    return True


def _expression_may_contain_sql_for_tables(expr: str) -> bool:
    """Heuristic: SSIS expressions are usually not SQL; only scan when SQL-like keywords appear."""
    if not expr or not isinstance(expr, str) or len(expr) < 12:
        return False
    u = expr.upper()
    if 'SELECT' in u and 'FROM' in u:
        return True
    if any(k in u for k in ('INSERT INTO', 'UPDATE ', 'DELETE FROM', 'MERGE ', 'JOIN ')):
        return True
    return False


def aggregate_package_referenced_tables(activities: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Collect unique table references from all SQL-bearing locations in the package:
    Execute SQL tasks, Data Flow sources/destinations, and transformations (e.g. Lookup).
    Returns merged list plus per-table provenance (where each table was referenced).
    """
    by_full_name: Dict[str, Dict[str, str]] = {}
    sources: Dict[str, List[str]] = {}

    def add_tables(tables: Optional[List[Dict[str, str]]], source_desc: str) -> None:
        for t in tables or []:
            if not isinstance(t, dict):
                continue
            fn = (t.get('fullName') or '').strip()
            if not fn:
                continue
            if fn not in by_full_name:
                by_full_name[fn] = {
                    'database': t.get('database') or '',
                    'schema': t.get('schema') or '',
                    'table': t.get('table') or '',
                    'fullName': fn,
                }
            if fn not in sources:
                sources[fn] = []
            if source_desc not in sources[fn]:
                sources[fn].append(source_desc)

    for act in activities:
        aname = act.get('name') or act.get('activityName') or ''
        atype = act.get('type') or ''
        stp = act.get('sqlTaskProperties') or {}
        if stp.get('referencedTables') and len(stp['referencedTables']) > 0:
            add_tables(stp['referencedTables'], f"Execute SQL: {aname}")
        elif atype == 'Execute SQL Task':
            sql = (act.get('sqlCommand') or stp.get('sqlStatementSource') or '').strip()
            if _sql_text_usable_for_table_extraction(sql):
                add_tables(extract_table_references(sql), f"Execute SQL: {aname}")

        for comp in act.get('components') or []:
            cname = comp.get('name') or ''
            sm = comp.get('sourceMetadata') or {}
            dm = comp.get('destinationMetadata') or {}
            tl = comp.get('transformationLogic') or {}
            if sm.get('referencedTables'):
                add_tables(sm['referencedTables'], f"Data Flow: {aname} > {cname} (Source)")
            if dm.get('referencedTables'):
                add_tables(dm['referencedTables'], f"Data Flow: {aname} > {cname} (Destination)")
            if tl.get('referencedTables'):
                add_tables(tl['referencedTables'], f"Data Flow: {aname} > {cname} (Transformation)")

    merged = sorted(by_full_name.values(), key=lambda x: (x.get('fullName') or '').lower())
    detailed = [
        {'table': by_full_name[fn], 'referencedFrom': sources.get(fn, [])}
        for fn in sorted(by_full_name.keys(), key=lambda s: s.lower())
    ]
    return {
        'packageReferencedTables': merged,
        'packageReferencedTablesDetailed': detailed,
    }


def extract_attribute(element, attr_name: str, default: str = "") -> str:
    """Extract attribute value from element with namespace handling."""
    value = element.get(f"{{www.microsoft.com/SqlServer/Dts}}{attr_name}")
    if value is None:
        value = element.get(attr_name)
    return value or default


def parse_connection_string(conn_string: str) -> Dict[str, str]:
    """Parse connection string into components."""
    components = {}
    if not conn_string:
        return components
    
    # Split by semicolon but respect quoted values
    parts = re.split(r';(?=(?:[^"]*"[^"]*")*[^"]*$)', conn_string)
    
    for part in parts:
        if '=' in part:
            key, value = part.split('=', 1)
            key = key.strip()
            value = value.strip()
            
            # Map common keys to our schema
            if key.lower() == 'data source':
                components['dataSource'] = value
            elif key.lower() == 'initial catalog':
                components['initialCatalog'] = value
            elif key.lower() == 'user id':
                components['userId'] = value
            elif key.lower() == 'provider':
                components['provider'] = value
    
    return components


def parse_variables(root) -> List[Dict[str, Any]]:
    """
    FR-2: Extract package and task variables from the DTSX package.
    Returns list of variables with Name, DataType, Default Value (and Namespace).
    Enhanced to extract full VariableValue text (for SQL queries), EvaluateAsExpression, and Expression.
    """
    variables = []
    # Find all DTS:Variable elements (package-level and nested under Executables)
    variable_elements = root.findall('.//DTS:Variable', DTS_NAMESPACE)
    for var_elem in variable_elements:
        name = extract_attribute(var_elem, 'ObjectName')
        namespace = extract_attribute(var_elem, 'Namespace')
        data_type_attr = extract_attribute(var_elem, 'DataType')
        # SSIS DataType: 2=Int16, 3=Int32, 4=Single, 5=Double, 6=Currency, 7=Date, 8=String, 11=Boolean, 17=Byte, 20=Int64, etc.
        data_type_map = {
            '2': 'Int16', '3': 'Int32', '4': 'Single', '5': 'Double', '6': 'Currency',
            '7': 'Date', '8': 'String', '11': 'Boolean', '17': 'Byte', '20': 'Int64',
            '72': 'GUID', '128': 'Object'
        }
        data_type = data_type_map.get(data_type_attr, f'System.{data_type_attr}' if data_type_attr else 'String')
        default_value = extract_attribute(var_elem, 'Value')
        if not default_value:
            val_elem = var_elem.find('.//DTS:VariableValue', DTS_NAMESPACE)
            if val_elem is not None:
                # Use full text content (itertext) for multi-line values like SQL queries
                full_text = ''.join(val_elem.itertext()).strip() if hasattr(val_elem, 'itertext') else (val_elem.text or '')
                default_value = full_text.strip() if isinstance(full_text, str) else (val_elem.text or '')
        expression = extract_attribute(var_elem, 'Expression')
        evaluate_as_expr = extract_attribute(var_elem, 'EvaluateAsExpression', 'False').lower() == 'true'
        variables.append({
            'name': name,
            'namespace': namespace or 'User',
            'dataType': data_type,
            'defaultValue': default_value or '',
            'expression': expression or '',
            'evaluateAsExpression': evaluate_as_expr,
        })
    return variables


def build_variable_lookup(variables: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Build a lookup dict for variable resolution.
    Key: "Namespace::Name" (e.g. "User::Qry"), Value: variable dict.
    Later definitions (deeper in hierarchy) override earlier - supports container-level variables.
    """
    lookup: Dict[str, Dict[str, Any]] = {}
    for var in variables:
        ns = var.get('namespace', 'User') or 'User'
        name = var.get('name', '')
        if name:
            key = f"{ns}::{name}"
            lookup[key] = var
    return lookup


def resolve_sql_from_variable(
    variable_ref: str,
    variable_lookup: Dict[str, Dict[str, Any]]
) -> tuple:
    """
    Resolve SqlCommandVariable reference (e.g. "User::Qry") to actual SQL query.
    Returns (resolved_sql: str or None, is_dynamic: bool, source_info: dict).
    - resolved_sql: The SQL when available (from defaultValue/design-time value); None if unresolved
    - is_dynamic: True when variable uses expression and we cannot fully evaluate at parse time
    - source_info: Metadata about the variable (variableRef, evaluateAsExpression, etc.)
    """
    if not variable_ref or not variable_ref.strip() or not variable_lookup:
        return (None, False, {})
    ref = variable_ref.strip()
    # Try exact match first
    var = variable_lookup.get(ref)
    if not var:
        # Try case-insensitive match
        ref_lower = ref.lower()
        for k, v in variable_lookup.items():
            if k.lower() == ref_lower:
                var = v
                break
    if not var:
        return (None, False, {'variableRef': ref, 'resolved': False, 'reason': 'Variable not found'})
    source_info = {
        'variableRef': ref,
        'variableName': var.get('name'),
        'namespace': var.get('namespace'),
        'evaluateAsExpression': var.get('evaluateAsExpression', False),
    }
    default_val = (var.get('defaultValue') or '').strip()
    is_dynamic = var.get('evaluateAsExpression', False)
    if default_val:
        # Use design-time/default value (SSIS stores evaluated value in VariableValue)
        source_info['resolved'] = True
        source_info['resolutionType'] = 'design_time_value' if is_dynamic else 'static_value'
        return (default_val, is_dynamic, source_info)
    if is_dynamic and var.get('expression'):
        # Expression-only: we cannot evaluate at parse time
        source_info['resolved'] = False
        source_info['reason'] = 'Dynamic expression - cannot resolve at parse time'
        source_info['expression'] = var.get('expression', '')
        return (None, True, source_info)
    return (None, False, source_info)


def parse_connection_managers(root) -> List[Dict[str, Any]]:
    """
    FR-2: Extract all connection managers from the DTSX package.
    Extracts: connection string, provider (for ADF/Databricks mapping).
    """
    connections = []
    
    conn_managers = root.findall('.//DTS:ConnectionManager', DTS_NAMESPACE)
    
    for conn in conn_managers:
        conn_id = extract_attribute(conn, 'refId')
        conn_name = extract_attribute(conn, 'ObjectName')
        creation_name = extract_attribute(conn, 'CreationName')
        
        # Get connection string: first from element, then from nested ConnectionManager in ObjectData
        conn_string = extract_attribute(conn, 'ConnectionString')
        if not conn_string:
            conn_manager_obj = conn.find('.//DTS:ConnectionManager', DTS_NAMESPACE)
            if conn_manager_obj is not None:
                conn_string = extract_attribute(conn_manager_obj, 'ConnectionString')
        
        # Parse connection string into components (includes provider)
        conn_components = parse_connection_string(conn_string)
        
        # Get DTSID for connection matching
        conn_dtsid = extract_attribute(conn, 'DTSID')
        
        connection = {
            'id': conn_id,
            'name': conn_name,
            'creationName': creation_name,
            'dtsId': conn_dtsid,
            'connectionString': conn_string,
            **conn_components
        }
        
        connections.append(connection)
    
    return connections


def parse_properties(element, property_path: str = './/property') -> List[Dict[str, Any]]:
    """Extract properties from an element. Uses text_content() for full value (e.g. long SqlCommand)."""
    properties = []
    
    prop_elements = element.findall(property_path)
    
    for prop in prop_elements:
        name = prop.get('name', '')
        data_type = prop.get('dataType', '')
        description = prop.get('description', '')
        
        # Get full property value: text_content() captures entire content (no truncation for long/multi-line e.g. SqlCommand)
        raw = prop.text_content() if hasattr(prop, 'text_content') else (prop.text or "")
        value = raw.strip() if isinstance(raw, str) else (raw or "")
        
        # Try to convert to appropriate type
        if data_type == 'System.Int32' or data_type == 'System.Int64':
            try:
                value = int(value) if value else 0
            except ValueError:
                pass
        elif data_type == 'System.Boolean':
            value = value.lower() == 'true' if value else False
        
        properties.append({
            'name': name,
            'value': value,
            'dataType': data_type,
            'description': description
        })
    
    return properties


def parse_column_mappings(columns_element, is_input: bool = True) -> List[Dict[str, Any]]:
    """Parse input or output column mappings."""
    columns = []
    
    if columns_element is None:
        return columns
    
    for col in columns_element:
        col_name = col.get('name', '')
        data_type = col.get('dataType', '')
        length = col.get('length')
        precision = col.get('precision')
        scale = col.get('scale')
        lineage_id = col.get('lineageId', '')
        
        column = {
            'name': col_name,
            'dataType': data_type,
            'lineageId': lineage_id
        }
        
        if length:
            try:
                column['length'] = int(length)
            except ValueError:
                pass
        
        if precision:
            try:
                column['precision'] = int(precision)
            except ValueError:
                pass
        
        if scale:
            try:
                column['scale'] = int(scale)
            except ValueError:
                pass
        
        columns.append(column)
    
    return columns


# FR-3/FR-4: SSIS component class ID to friendly name and PySpark equivalent
COMPONENT_TYPE_MAP = {
    'Microsoft.OLEDBSource': ('OLE DB Source', 'Source', None),
    'Microsoft.OLEDBDestination': ('OLE DB Destination', 'Destination', None),
    'Microsoft.ADONETSource': ('ADO.NET Source', 'Source', None),
    'Microsoft.ADONETDestination': ('ADO.NET Destination', 'Destination', None),
    'Microsoft.FlatFileSource': ('Flat File Source', 'Source', None),
    'Microsoft.FlatFileDestination': ('Flat File Destination', 'Destination', None),
    'Microsoft.DerivedColumn': ('Derived Column', 'Transformation', 'withColumn'),
    'Microsoft.Lookup': ('Lookup', 'Transformation', 'join'),
    'Microsoft.Aggregate': ('Aggregate', 'Transformation', 'groupBy'),
    'Microsoft.ConditionalSplit': ('Conditional Split', 'Transformation', 'when/otherwise'),
    'Microsoft.DataConvert': ('Data Conversion', 'Transformation', 'cast()'),
    'Microsoft.Sort': ('Sort', 'Transformation', 'orderBy'),
    'Microsoft.Multicast': ('Multicast', 'Transformation', None),
    'Microsoft.ManagedComponentHost': ('Script Task', 'Transformation', None),
}
# Script Task / ManagedComponentHost: flag for manual review
REQUIRES_MANUAL_REVIEW_COMPONENTS = {'Microsoft.ManagedComponentHost', 'Microsoft.ScriptComponent'}


def _parse_schema_table(full_name: str) -> tuple:
    """Parse [SchemaName].[TableName] or SchemaName.TableName into (schema, table)."""
    if not full_name or not isinstance(full_name, str):
        return ('', '')
    s = full_name.strip()
    # Remove brackets and split by ].[
    if s.startswith('[') and '].[' in s and s.endswith(']'):
        parts = s[1:-1].split('].[')
        if len(parts) >= 2:
            return (parts[0].strip(), parts[1].strip())
        if len(parts) == 1:
            return ('', parts[0].strip())
    # Fallback: split by dot
    if '.' in s:
        idx = s.rfind('.')
        return (s[:idx].strip().strip('[]'), s[idx + 1:].strip().strip('[]'))
    return ('', s.strip('[]'))


def _extract_derived_column_expressions(comp_elem) -> List[Dict[str, Any]]:
    """FR-4: Extract column mappings and expressions from Derived Column (and similar) output columns."""
    expressions = []
    for out in (comp_elem.findall('.//output') or []):
        for out_col in (out.findall('outputColumns/outputColumn') or out.findall('.//outputColumn') or []):
            col_name = out_col.get('name', '')
            props = out_col.find('properties')
            if props is None:
                props = out_col.find('.//properties')
            if props is None:
                continue
            expr_val = None
            friendly_val = None
            for p in props.findall('property'):
                if p.get('name') == 'Expression':
                    expr_val = (p.text or '').strip()
                elif p.get('name') == 'FriendlyExpression':
                    friendly_val = (p.text or '').strip()
            if expr_val or col_name:
                expressions.append({
                    'outputColumn': col_name,
                    'expression': expr_val or '',
                    'friendlyExpression': friendly_val or expr_val or ''
                })
    return expressions


def parse_pipeline_paths(pipeline_element) -> List[Dict[str, Any]]:
    """FR-4: Parse pipeline path elements to capture transformation sequence (source -> destination)."""
    if pipeline_element is None:
        return []
    paths = []
    for path_elem in pipeline_element.findall('.//path'):
        start_id = path_elem.get('startId', '')
        end_id = path_elem.get('endId', '')
        name = path_elem.get('name', '')
        if start_id or end_id:
            paths.append({
                'startId': start_id,
                'endId': end_id,
                'name': name,
                'description': f'{start_id} -> {end_id}'
            })
    return paths


def parse_data_flow_components(
    pipeline_element,
    variable_lookup: Optional[Dict[str, Dict[str, Any]]] = None
) -> List[Dict[str, Any]]:
    """
    FR-3 / FR-4: Parse data flow components from a pipeline element.
    Detects: OLE DB Source, ADO.NET Source, Flat File Source, OLE DB Destination,
    Lookup, Derived Column, Aggregate, Conditional Split, Data Conversion, Sort, Script Task.
    Extracts source metadata (SourceSchemaName, SourceTableName, SourceQuery, SourceID),
    destination metadata (TargetTableName, TargetSchemaName, TargetDBName, CopyMode),
    and transformation logic (pysparkEquivalent, column mappings, expressions, sequence).
    When SqlCommand is empty and SqlCommandVariable is set, resolves the variable to get the SQL query.
    """
    components = []
    if pipeline_element is None:
        return components
    
    variable_lookup = variable_lookup or {}
    component_elements = pipeline_element.findall('.//component')
    source_counter = 0
    
    for comp in component_elements:
        comp_id = comp.get('refId', '')
        comp_name = comp.get('name', '')
        comp_class = comp.get('componentClassID', '')
        description = comp.get('description', '')
        
        # FR-3: Resolve component type from class ID (friendly name, category, PySpark equivalent)
        type_info = COMPONENT_TYPE_MAP.get(comp_class)
        if type_info:
            friendly_name, comp_type, pyspark_equivalent = type_info
        else:
            if 'Source' in comp_class or 'Source' in comp_name:
                comp_type, friendly_name, pyspark_equivalent = 'Source', comp_class.split('.')[-1], None
            elif 'Destination' in comp_class or 'Destination' in comp_name:
                comp_type, friendly_name, pyspark_equivalent = 'Destination', comp_class.split('.')[-1], None
            else:
                comp_type, friendly_name = 'Transformation', comp_class.split('.')[-1] if '.' in comp_class else comp_class
                pyspark_equivalent = None
        
        # FR-3: Flag Script Task / ManagedComponentHost for manual review
        requires_manual_review = comp_class in REQUIRES_MANUAL_REVIEW_COMPONENTS
        
        # Get component properties
        properties = parse_properties(comp, './/properties/property')
        prop_dict = {p['name']: p.get('value', '') for p in properties}
        
        sql_command = prop_dict.get('SqlCommand', '') or None
        # Explicitly extract full SqlCommand from XML (varchar(max) - entire query, no truncation)
        for prop_elem in comp.findall('.//property') or comp.findall('.//properties/property') or []:
            if (prop_elem.get('name') or '').strip() == 'SqlCommand':
                full_text = ''.join(prop_elem.itertext()).strip()
                if full_text:
                    sql_command = full_text
                break
        # SqlCommandVariable: resolve variable when SqlCommand is empty
        sql_command_variable_ref = (prop_dict.get('SqlCommandVariable') or '').strip()
        sql_command_source_info = None
        if not sql_command and sql_command_variable_ref and variable_lookup:
            resolved_sql, is_dynamic, source_info = resolve_sql_from_variable(sql_command_variable_ref, variable_lookup)
            sql_command_source_info = source_info
            if resolved_sql:
                sql_command = resolved_sql
            elif is_dynamic and source_info.get('expression'):
                # Dynamic/unresolved: store placeholder for UI to display
                sql_command = f"[Dynamic query - Variable: {sql_command_variable_ref}]\nExpression: {source_info.get('expression', '')[:200]}..."
        open_rowset = (prop_dict.get('OpenRowset') or '').strip()
        
        # FR-2+: Extract ALL connections from component (not just first one) with full metadata
        # This captures connections used in Lookup, OLE DB Source, OLE DB Destination, etc.
        component_connections = []
        connection_id = None  # Set to first connection for backward compatibility
        
        for conn_elem in comp.findall('.//connections/connection'):
            conn_ref_id = conn_elem.get('connectionManagerRefId') or conn_elem.get('connectionManagerID')
            conn_name = conn_elem.get('name', '')
            conn_description = conn_elem.get('description', '')
            conn_id = conn_elem.get('refId', '')
            
            if conn_ref_id:
                conn_info = {
                    'connectionManagerRefId': conn_ref_id,
                    'connectionManagerID': conn_elem.get('connectionManagerID', ''),
                    'name': conn_name,
                    'description': conn_description,
                    'refId': conn_id,
                }
                component_connections.append(conn_info)
                
                # Set primary connection_id from first connection (for backward compatibility)
                if not connection_id:
                    connection_id = conn_ref_id
        
        # Input / output columns
        input_columns = []
        for inp in comp.findall('.//inputs/input'):
            ic = inp.find('inputColumns')
            if ic is not None:
                input_columns.extend(parse_column_mappings(ic, True))
        output_columns = []
        for out in comp.findall('.//outputs/output'):
            oc = out.find('outputColumns')
            if oc is not None:
                output_columns.extend(parse_column_mappings(oc, False))
        
        component = {
            'id': comp_id,
            'name': comp_name,
            'componentClassID': comp_class,
            'componentType': comp_type,
            'componentTypeName': friendly_name,
            'description': description,
            'properties': properties,
            'inputColumns': input_columns,
            'outputColumns': output_columns,
            'requiresManualReview': requires_manual_review,
        }
        if connection_id:
            component['connectionId'] = connection_id
        
        # FR-2+: Include all component connections (new field for enhanced visibility)
        if component_connections:
            component['componentConnections'] = component_connections
        
        # FR-3: Source metadata (SourceSchemaName, SourceTableName, SourceQuery, SourceID)
        if comp_type == 'Source':
            source_counter += 1
            schema_name, table_name = _parse_schema_table(open_rowset) if open_rowset else ('', '')
            component['sourceMetadata'] = {
                'sourceID': source_counter,
                'sourceSchemaName': schema_name,
                'sourceTableName': table_name or (open_rowset if open_rowset else ''),
                'sourceQuery': sql_command or '',
                'openRowset': open_rowset,
            }
            if sql_command:
                component['sourceMetadata']['sourceQuery'] = sql_command
            # Record when query came from SqlCommandVariable (for UI visibility)
            if sql_command_variable_ref:
                component['sourceMetadata']['sqlCommandVariableRef'] = sql_command_variable_ref
            if sql_command_source_info:
                component['sourceMetadata']['sqlCommandSourceInfo'] = sql_command_source_info
            
            # Extract table references from the source query
            if sql_command:
                referenced_tables = extract_table_references(sql_command)
                if referenced_tables:
                    component['sourceMetadata']['referencedTables'] = referenced_tables
        
        # FR-3: Destination metadata (TargetTableName, TargetSchemaName, TargetDBName, CopyMode)
        if comp_type == 'Destination':
            schema_name, table_name = _parse_schema_table(open_rowset) if open_rowset else ('', '')
            copy_mode = 'Incremental' if (sql_command and ('WHERE' in (sql_command or '').upper() or '@' in (sql_command or ''))) else 'Full'
            component['destinationMetadata'] = {
                'targetSchemaName': schema_name,
                'targetTableName': table_name or (open_rowset if open_rowset else ''),
                'targetDBName': '',  # Resolved from connectionDetails later
                'copyMode': copy_mode,
                'openRowset': open_rowset,
                'sqlCommand': sql_command or '',
            }
            component['tableName'] = open_rowset or table_name or ''
            
            # Extract table references from destination queries if present
            if sql_command:
                referenced_tables = extract_table_references(sql_command)
                if referenced_tables:
                    component['destinationMetadata']['referencedTables'] = referenced_tables
        
        # FR-4: Transformation logic (pysparkEquivalent, column mappings, expressions)
        if comp_type == 'Transformation':
            expressions = _extract_derived_column_expressions(comp)
            component['transformationLogic'] = {
                'pysparkEquivalent': pyspark_equivalent,
                'columnMappings': [{'outputColumn': e['outputColumn'], 'expression': e['expression'], 'friendlyExpression': e['friendlyExpression']} for e in expressions],
                'expressions': expressions,
            }
            if not expressions and pyspark_equivalent:
                component['transformationLogic']['columnMappings'] = [{'outputColumn': c.get('name'), 'expression': '', 'friendlyExpression': ''} for c in output_columns]
            # Referenced tables from SQL (Lookup SqlCommand, ADO.NET Lookup, etc.)
            ref_from_sql: List[Dict[str, str]] = []
            if sql_command and _sql_text_usable_for_table_extraction(sql_command):
                ref_from_sql = extract_table_references(sql_command)
            # Rare: embedded SQL in Derived Column / Conditional Split expressions
            expr_sql_chunks: List[str] = []
            for e in expressions:
                for key in ('expression', 'friendlyExpression'):
                    val = (e.get(key) or '')
                    if _expression_may_contain_sql_for_tables(val):
                        expr_sql_chunks.append(val)
            ref_tables = ref_from_sql
            if expr_sql_chunks:
                ref_tables = merge_referenced_tables_lists(
                    ref_from_sql,
                    extract_table_references('\n'.join(expr_sql_chunks)),
                )
            if ref_tables:
                component['transformationLogic']['referencedTables'] = ref_tables
        
        components.append(component)
    
    return components


def _collect_executables_with_parent(executables_elem, parent_ref_id: Optional[str]) -> list:
    """
    Step 1 & 9: Recursively collect all DTS:Executable elements with parent container refId.
    Returns list of (exe_element, parent_ref_id).
    """
    result = []
    if executables_elem is None:
        return result
    for exe in executables_elem.findall('DTS:Executable', DTS_NAMESPACE):
        result.append((exe, parent_ref_id))
        nested = exe.find('DTS:Executables', DTS_NAMESPACE)
        if nested is not None:
            exe_id = extract_attribute(exe, 'refId')
            result.extend(_collect_executables_with_parent(nested, exe_id))
    return result


def parse_activities(root, variables: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
    """
    Step 1: Extract all control flow activities (including nested in Sequence Containers).
    Each activity includes parentContainerRefId when inside a container.
    When variables is provided, uses them to resolve SqlCommandVariable in Data Flow components.
    """
    activities = []
    variable_lookup = build_variable_lookup(variables or parse_variables(root))
    executables_container = root.find('DTS:Executables', DTS_NAMESPACE)
    package_ref_id = extract_attribute(root, 'refId') or 'Package'
    if executables_container is None:
        return activities
    executable_pairs = _collect_executables_with_parent(executables_container, package_ref_id)
    
    for exe, parent_ref_id in executable_pairs:
        exe_id = extract_attribute(exe, 'refId')
        exe_name = extract_attribute(exe, 'ObjectName')
        exe_type = extract_attribute(exe, 'ExecutableType')
        description = extract_attribute(exe, 'Description')
        disabled = extract_attribute(exe, 'Disabled', 'False').lower() == 'true'
        creation_name = extract_attribute(exe, 'CreationName')
        
        # FR-2: Determine activity type (Package Name, Execute SQL, Data Flow, Sequence Containers, Precedence)
        activity_type = 'Unknown'
        if 'Pipeline' in exe_type or 'Pipeline' in creation_name:
            activity_type = 'Data Flow Task'
        elif 'SQL' in exe_type or 'SQL' in creation_name:
            activity_type = 'Execute SQL Task'
        elif 'ExecutePackageTask' in exe_type or 'ExecutePackageTask' in creation_name:
            activity_type = 'Execute Package Task'
        elif 'SEQUENCE' in (exe_type or '') or (creation_name or '').upper() == 'STOCK:SEQUENCE':
            activity_type = 'Sequence Container'
        elif 'Script' in exe_type or 'Script' in creation_name:
            activity_type = 'Script Task'
        else:
            activity_type = exe_type.split('.')[-1] if (exe_type and '.' in exe_type) else (exe_type or creation_name or 'Unknown')
        
        # Get properties
        properties = []
        
        # For SQL tasks, extract all critical properties
        sql_command = None
        sql_task_properties = {}
        connection_id = None
        
        if 'SQL' in exe_type or activity_type == 'Execute SQL Task':
            # Look for SQL statement in ObjectData
            sql_task = exe.find('.//DTS:ObjectData', DTS_NAMESPACE)
            if sql_task is not None:
                # Try different namespace variations for SQLTask
                sql_namespaces = [
                    {'SQLTask': 'www.microsoft.com/sqlserver/dts/tasks/sqltask'},
                    {'SQLTask': 'http://schemas.microsoft.com/sqlserver/dts/tasks/sqltask'},
                ]
                
                sql_elem = None
                working_ns_uri = None
                for ns in sql_namespaces:
                    sql_elem = sql_task.find('.//SQLTask:SqlTaskData', ns)
                    if sql_elem is not None:
                        working_ns_uri = ns.get('SQLTask', '')
                        break
                
                if sql_elem is not None:
                    # Extract critical SQL Task properties
                    # In lxml, namespaced attributes are accessed using {namespace}localname format
                    # SQLTask:Connection becomes {www.microsoft.com/sqlserver/dts/tasks/sqltask}Connection
                    
                    # Get all attributes for easier searching
                    all_attrs = sql_elem.attrib
                    
                    # Get SQL Statement Source
                    sql_command = None
                    if working_ns_uri:
                        # Try namespace-qualified attribute
                        sql_command = all_attrs.get(f"{{{working_ns_uri}}}SqlStatementSource")
                    # If not found, search all attributes
                    if not sql_command:
                        for attr_name, attr_value in all_attrs.items():
                            # Check if attribute name contains SqlStatementSource
                            if 'SqlStatementSource' in attr_name and 'xmlns' not in attr_name:
                                sql_command = attr_value
                                break
                    
                    # Decode HTML entities in SQL command
                    if sql_command:
                        import html
                        sql_command = html.unescape(sql_command)
                        # Replace &#xA; with newlines
                        sql_command = sql_command.replace('&#xA;', '\n')
                        sql_task_properties['sqlStatementSource'] = sql_command
                    
                    # Connection Manager Reference
                    connection_ref = None
                    if working_ns_uri:
                        connection_ref = all_attrs.get(f"{{{working_ns_uri}}}Connection")
                    if not connection_ref:
                        # Search in all attributes
                        for attr_name, attr_value in all_attrs.items():
                            if 'Connection' in attr_name and 'xmlns' not in attr_name:
                                connection_ref = attr_value
                                break
                    
                    if connection_ref:
                        sql_task_properties['connection'] = connection_ref
                        connection_id = connection_ref
                    
                    # SQL Statement Source Type (DirectInput, FileConnection, Variable)
                    # Default to DirectInput if SQL statement is present
                    if sql_command:
                        sql_source_type = None
                        if working_ns_uri:
                            sql_source_type = all_attrs.get(f"{{{working_ns_uri}}}SqlStatementSourceType")
                        if not sql_source_type:
                            # Search in attributes
                            for attr_name, attr_value in all_attrs.items():
                                if 'SqlStatementSourceType' in attr_name and 'xmlns' not in attr_name:
                                    sql_source_type = attr_value
                                    break
                        sql_task_properties['sqlStatementSourceType'] = sql_source_type if sql_source_type else 'DirectInput'
                    
                    # Timeout (in seconds) - default to 0 if not specified
                    timeout = None
                    if working_ns_uri:
                        timeout = all_attrs.get(f"{{{working_ns_uri}}}TimeOut")
                    if not timeout:
                        for attr_name, attr_value in all_attrs.items():
                            if 'TimeOut' in attr_name and 'xmlns' not in attr_name:
                                timeout = attr_value
                                break
                    sql_task_properties['timeout'] = timeout if timeout else '0'
                    
                    # Code Page (for SQL statements)
                    if working_ns_uri:
                        code_page = all_attrs.get(f"{{{working_ns_uri}}}CodePage")
                        if code_page:
                            sql_task_properties['codePage'] = code_page
                        else:
                            # Search by name pattern
                            for attr_name, attr_value in all_attrs.items():
                                if 'CodePage' in attr_name and 'xmlns' not in attr_name:
                                    sql_task_properties['codePage'] = attr_value
                                    break
                    
                    # Bypass Prepare (boolean) - default to False
                    bypass_prepare = None
                    if working_ns_uri:
                        bypass_prepare = all_attrs.get(f"{{{working_ns_uri}}}BypassPrepare")
                    if not bypass_prepare:
                        for attr_name, attr_value in all_attrs.items():
                            if 'BypassPrepare' in attr_name and 'xmlns' not in attr_name:
                                bypass_prepare = attr_value
                                break
                    sql_task_properties['bypassPrepare'] = bypass_prepare.lower() == 'true' if bypass_prepare else False
                    
                    # Result Set Type (None, SingleRow, Full, XML) - default to None
                    result_set = None
                    if working_ns_uri:
                        result_set = all_attrs.get(f"{{{working_ns_uri}}}ResultSetType")
                    if not result_set:
                        for attr_name, attr_value in all_attrs.items():
                            if 'ResultSetType' in attr_name and 'xmlns' not in attr_name:
                                result_set = attr_value
                                break
                    sql_task_properties['resultSetType'] = result_set if result_set else 'None'
                    
                    # IsStoredProcedure (boolean) - default to False
                    is_stored_proc = None
                    if working_ns_uri:
                        is_stored_proc = all_attrs.get(f"{{{working_ns_uri}}}IsStoredProcedure")
                    if not is_stored_proc:
                        for attr_name, attr_value in all_attrs.items():
                            if 'IsStoredProcedure' in attr_name and 'xmlns' not in attr_name:
                                is_stored_proc = attr_value
                                break
                    sql_task_properties['isStoredProcedure'] = is_stored_proc.lower() == 'true' if is_stored_proc else False
                    
                    # Parameter Bindings
                    parameter_bindings = []
                    # Use the namespace that worked for finding sql_elem
                    working_ns = None
                    for ns in sql_namespaces:
                        test_elem = sql_task.find('.//SQLTask:SqlTaskData', ns)
                        if test_elem is not None:
                            working_ns = ns
                            break
                    
                    if working_ns:
                        param_bindings = sql_elem.findall('.//SQLTask:ParameterBinding', working_ns)
                        for param in param_bindings:
                            param_name = extract_attribute(param, 'SQLTask:ParameterName')
                            param_direction = extract_attribute(param, 'SQLTask:ParameterDirection')
                            param_data_type = extract_attribute(param, 'SQLTask:ParameterDataType')
                            param_size = extract_attribute(param, 'SQLTask:ParameterSize')
                            param_variable = extract_attribute(param, 'SQLTask:DtsVariableName')
                            
                            if param_name:
                                parameter_bindings.append({
                                    'name': param_name,
                                    'direction': param_direction,
                                    'dataType': param_data_type,
                                    'size': param_size,
                                    'variableName': param_variable
                                })
                        
                        if parameter_bindings:
                            sql_task_properties['parameterBindings'] = parameter_bindings
                        
                        # Result Set Bindings
                        result_bindings = []
                        result_bindings_elem = sql_elem.findall('.//SQLTask:ResultBinding', working_ns)
                        for binding in result_bindings_elem:
                            result_name = extract_attribute(binding, 'SQLTask:ResultName')
                            result_variable = extract_attribute(binding, 'SQLTask:DtsVariableName')
                            
                            if result_name:
                                result_bindings.append({
                                    'resultName': result_name,
                                    'variableName': result_variable
                                })
                        
                        if result_bindings:
                            sql_task_properties['resultBindings'] = result_bindings
        
        # For Execute Package tasks, extract PackageName and related properties
        execute_package_properties = {}
        if 'ExecutePackageTask' in exe_type or activity_type == 'Execute Package Task':
            # Look for ExecutePackageTask in ObjectData
            object_data = exe.find('.//DTS:ObjectData', DTS_NAMESPACE)
            if object_data is not None:
                # Find ExecutePackageTask element (no namespace typically)
                execute_package_task = object_data.find('.//ExecutePackageTask')
                if execute_package_task is None:
                    # Try with namespace variations
                    execute_package_task = object_data.find('.//{*}ExecutePackageTask')
                
                if execute_package_task is not None:
                    # Extract PackageName
                    package_name_elem = execute_package_task.find('PackageName')
                    if package_name_elem is not None and package_name_elem.text:
                        execute_package_properties['packageName'] = package_name_elem.text.strip()
                    
                    # Extract UseProjectReference
                    use_project_ref_elem = execute_package_task.find('UseProjectReference')
                    if use_project_ref_elem is not None and use_project_ref_elem.text:
                        execute_package_properties['useProjectReference'] = use_project_ref_elem.text.strip().lower() == 'true'
                    
                    # Extract ParameterAssignments
                    parameter_assignments = []
                    param_assignment_elems = execute_package_task.findall('ParameterAssignment')
                    for param_elem in param_assignment_elems:
                        param_name_elem = param_elem.find('ParameterName')
                        binded_var_elem = param_elem.find('BindedVariableOrParameterName')
                        
                        param_assignment = {}
                        if param_name_elem is not None and param_name_elem.text:
                            param_assignment['parameterName'] = param_name_elem.text.strip()
                        if binded_var_elem is not None and binded_var_elem.text:
                            param_assignment['bindedVariableOrParameterName'] = binded_var_elem.text.strip()
                        
                        if param_assignment:
                            parameter_assignments.append(param_assignment)
                    
                    if parameter_assignments:
                        execute_package_properties['parameterAssignments'] = parameter_assignments
        
        # FR-3/FR-4: For Data Flow tasks, parse pipeline components and paths (transformation sequence)
        components = []
        data_flow_paths = []
        if 'Pipeline' in exe_type or 'Pipeline' in creation_name:
            pipeline = exe.find('.//pipeline')
            if pipeline is not None:
                components = parse_data_flow_components(pipeline, variable_lookup=variable_lookup)
                data_flow_paths = parse_pipeline_paths(pipeline)
                for comp in components:
                    for prop in comp.get('properties', []):
                        if prop['name'] == 'SqlCommand' and prop.get('value'):
                            sql_command = prop['value']
        
        # Get general properties from DTS:Property elements
        dts_properties = exe.findall('.//DTS:Property', DTS_NAMESPACE)
        for prop in dts_properties:
            prop_name = extract_attribute(prop, 'Name')
            prop_value = prop.text or ""
            
            if prop_name and prop_value:
                properties.append({
                    'name': prop_name,
                    'value': prop_value,
                    'dataType': 'System.String'
                })
        
        activity = {
            'id': exe_id,
            'name': exe_name,
            'type': activity_type,
            'executableType': exe_type,
            'description': description,
            'disabled': disabled,
            'properties': properties,
            'activityId': exe_id,
            'activityName': exe_name,
            'activityType': exe_type or activity_type,
        }
        if parent_ref_id:
            activity['parentContainerRefId'] = parent_ref_id
        
        # Add SQL command (for backward compatibility)
        if sql_command:
            activity['sqlCommand'] = sql_command
        
        # Add comprehensive SQL Task properties
        if sql_task_properties:
            # Extract table references from SQL statement if present
            if sql_command:
                referenced_tables = extract_table_references(sql_command)
                if referenced_tables:
                    sql_task_properties['referencedTables'] = referenced_tables
            activity['sqlTaskProperties'] = sql_task_properties
        
        # Add Execute Package Task properties
        if execute_package_properties:
            activity['executePackageTaskProperties'] = execute_package_properties
        
        # Add connection ID if found
        if connection_id:
            activity['connectionId'] = connection_id
        
        if components:
            activity['components'] = components
        if data_flow_paths:
            activity['dataFlowPaths'] = data_flow_paths
        
        activities.append(activity)
    
    return activities


def build_execution_sequence(
    activities: List[Dict[str, Any]],
    precedence_map: Dict[str, List[str]]
) -> List[Dict[str, Any]]:
    """
    FR-2: Build execution sequence (ordered list) from precedence constraints.
    Returns list of { refId, name, type, order } in execution order.
    """
    ref_ids = [a['id'].strip() for a in activities]
    id_to_activity = {a['id'].strip(): a for a in activities}
    # in_degree[to_id] = number of predecessors (from_activities that must run first)
    in_degree = {rid: 0 for rid in ref_ids}
    for to_id, from_list in precedence_map.items():
        to_id = to_id.strip()
        if to_id in in_degree:
            in_degree[to_id] = len([f for f in from_list if f.strip()])
    # Topological order: start with activities that have no predecessors
    queue = [rid for rid in ref_ids if in_degree[rid] == 0]
    order_list = []
    seen = set()
    while queue:
        rid = queue.pop(0)
        if rid in seen:
            continue
        seen.add(rid)
        act = id_to_activity.get(rid)
        if act:
            order_list.append({
                'refId': rid,
                'name': act.get('name', ''),
                'type': act.get('type', ''),
                'order': len(order_list) + 1
            })
        # Find all activities that have rid as a predecessor
        for to_id, from_list in precedence_map.items():
            to_id = to_id.strip()
            if rid in [f.strip() for f in from_list] and to_id in in_degree and to_id not in seen:
                in_degree[to_id] = in_degree[to_id] - 1
                if in_degree[to_id] <= 0:
                    queue.append(to_id)
    # Append any not reached (orphan or cycle)
    for rid in ref_ids:
        if rid not in seen:
            act = id_to_activity.get(rid)
            if act:
                order_list.append({
                    'refId': rid,
                    'name': act.get('name', ''),
                    'type': act.get('type', ''),
                    'order': len(order_list) + 1
                })
    return order_list


def parse_precedence_constraints(root) -> Dict[str, List[str]]:
    """
    Parse precedence constraints to build a mapping of activity refId to list of previous activity refIds.
    Returns a dictionary where key is the 'To' activity refId and value is a list of 'From' activity refIds.
    """
    _, detail = parse_precedence_constraints_detailed(root)
    return detail


def parse_precedence_constraints_detailed(root) -> tuple:
    """
    Step 2 & 3: Extract precedence constraints with full detail (DTS:From, DTS:To, DTS:Value, DTS:LogicalAnd).
    Returns (precedence_map, constraints_detail).
    precedence_map: Dict[To_refId, List[From_refId]] for backward compatibility.
    constraints_detail: List[{"fromRefId", "toRefId", "value", "logicalAnd", "constraintName"}] for flow graph.
    """
    constraints_map: Dict[str, List[str]] = {}
    constraints_detail: List[Dict[str, Any]] = []
    
    constraints = root.findall('.//DTS:PrecedenceConstraints/DTS:PrecedenceConstraint', DTS_NAMESPACE)
    
    for constraint in constraints:
        from_activity = extract_attribute(constraint, 'From')
        to_activity = extract_attribute(constraint, 'To')
        if not from_activity:
            from_activity = constraint.get('From') or constraint.get('{www.microsoft.com/SqlServer/Dts}From') or ''
        if not to_activity:
            to_activity = constraint.get('To') or constraint.get('{www.microsoft.com/SqlServer/Dts}To') or ''
        
        value_attr = extract_attribute(constraint, 'Value')
        if not value_attr:
            value_attr = constraint.get('Value') or constraint.get('{www.microsoft.com/SqlServer/Dts}Value') or 'Success'
        logical_and_attr = extract_attribute(constraint, 'LogicalAnd')
        if not logical_and_attr:
            logical_and_attr = constraint.get('LogicalAnd') or constraint.get('{www.microsoft.com/SqlServer/Dts}LogicalAnd') or 'True'
        constraint_name = extract_attribute(constraint, 'ObjectName') or ''
        
        if from_activity and to_activity:
            from_activity = from_activity.strip()
            to_activity = to_activity.strip()
            if to_activity not in constraints_map:
                constraints_map[to_activity] = []
            constraints_map[to_activity].append(from_activity)
            constraints_detail.append({
                'fromRefId': from_activity,
                'toRefId': to_activity,
                'value': value_attr if value_attr else 'Success',
                'logicalAnd': logical_and_attr.lower() == 'true',
                'constraintName': constraint_name,
            })
    
    return (constraints_map, constraints_detail)


def _escape_sql_nvarchar(s: str, max_len: Optional[int] = None) -> str:
    """Escape single quotes for SQL Server N'...' string literal."""
    if s is None:
        return "NULL"
    t = str(s).replace("'", "''")
    if max_len:
        t = t[:max_len]
    return f"N'{t}'"


def _detect_lookup_or_scd2(components: List[Dict[str, Any]]) -> bool:
    """Return True if Data Flow contains Lookup or dimension-like logic (SCD Type 2)."""
    for comp in components:
        cid = (comp.get('componentClassID') or '') if isinstance(comp, dict) else ''
        name = (comp.get('name') or '').lower() if isinstance(comp, dict) else ''
        if 'Lookup' in cid or 'lookup' in name:
            return True
    return False


def _infer_watermark_from_query(source_query: str) -> tuple:
    """Infer watermark column name and type from SQL WHERE clause if possible. Returns (name, type)."""
    if not source_query or not isinstance(source_query, str):
        return ('', '')
    q = source_query.upper()
    # Common patterns: WHERE ModifiedDate > ?, WHERE [ModifiedDate] > @var, etc.
    import re
    # Match column name before > or >= (simple heuristic)
    m = re.search(r'WHERE\s+\[?(\w+)\]?\s*[><=]', q, re.IGNORECASE | re.DOTALL)
    if m:
        col = m.group(1)
        if any(x in col for x in ('DATE', 'TIME', 'MODIFIED', 'UPDATED', 'CREATED')):
            return (col, 'DateTime')
        if any(x in col for x in ('ID', 'SEQ', 'NUM')):
            return (col, 'Int')
    return ('', '')


def generate_control_table_sql(parsed_data: Dict[str, Any]) -> str:
    """
    Generate SQL INSERT scripts for ADF metadata table dbo.ControlTableIntegrated.
    Uses all 25 columns per metadata table specification.
    TableID is omitted (assume IDENTITY); all other columns are populated from SSIS or defaults.
    """
    metadata = parsed_data.get('metadata', {})
    package_name = metadata.get('packageName', metadata.get('objectName', 'SSISPackage'))
    # AdfJobName: derived from SSIS Package Name
    adf_job_name = package_name.replace("'", "''")

    col_list = [
        "AdfJobName", "SourceSchemaName", "SourceTableName", "SourceID", "SourceQuery",
        "TargetTableName", "TargetSchemaName", "TargetDBName", "DBXSCDType", "DBXPipelineId",
        "JobId", "ADLSContainerName", "CopyMode", "DeltaColumnName", "IsActive",
        "WaterMarkColumnName", "WaterMarkColumnType", "FolderPath", "PartitionColumnName",
        "PartitionLowerBound", "PartitionUpperBound", "MaskingSchemas", "IndField", "IndValue",
    ]

    col_list_str = ", ".join(col_list)
    header_lines = [
        "-- Generated from SSIS package: " + package_name,
        "-- Target: dbo.ControlTableIntegrated (ADF metadata-driven pipeline)",
        "-- TableID: assumed IDENTITY(1,1) on table, not inserted.",
        "-- SourceID: connection manager name of the source activity (from SSIS XML).",
        "-- SourceQuery column: varchar(max); full SQL is emitted (no splitting).",
        "-- One INSERT per entry.",
        "",
    ]

    connection_managers = parsed_data.get('connectionManagers', [])
    insert_statements = []
    table_id = 0
    for activity in parsed_data.get('activities', []):
        if activity.get('type') != 'Data Flow Task' or not activity.get('components'):
            continue
        task_name = activity.get('name', '')
        components = activity['components']
        has_lookup_scd2 = _detect_lookup_or_scd2(components)

        for comp in components:
            if comp.get('componentType') != 'Source' or not comp.get('sourceMetadata'):
                continue
            sm = comp['sourceMetadata']
            dest_meta = None
            for c2 in components:
                if c2.get('destinationMetadata'):
                    dest_meta = c2['destinationMetadata']
                    break
            if not dest_meta:
                dest_meta = {
                    'targetSchemaName': '', 'targetTableName': '', 'targetDBName': '',
                    'copyMode': 'Full',
                }

            table_id += 1
            src_schema = (sm.get('sourceSchemaName') or '') or ''
            src_table = (sm.get('sourceTableName') or '') or ''
            # SourceID = connection manager name of the source activity (from SSIS XML)
            connection_id = (comp.get('connectionId') or '').strip()
            conn_details = resolve_connection_details(connection_id, connection_managers) if connection_id else None
            if conn_details:
                src_id = (conn_details.get('name') or '').strip()
            elif connection_id and '[' in connection_id and ']' in connection_id:
                # Project/Package refId format: "Project.ConnectionManagers[OLEDB.SA_Staging]" -> use name in brackets
                src_id = connection_id.split('[')[1].split(']')[0].strip()
            else:
                src_id = (sm.get('sourceID') or '')
                if not src_id and isinstance(sm.get('sourceID'), (int, float)):
                    src_id = str(int(sm['sourceID']))
                elif isinstance(src_id, (int, float)):
                    src_id = str(int(src_id))
            if not isinstance(src_id, str):
                src_id = str(src_id) if src_id else ''
            source_query = (sm.get('sourceQuery') or '') or ''
            tgt_schema = (dest_meta.get('targetSchemaName') or '') or ''
            tgt_table = (dest_meta.get('targetTableName') or '') or ''
            tgt_db = (dest_meta.get('targetDBName') or '') or ''
            copy_mode = (dest_meta.get('copyMode') or 'Full') or 'Full'
            if copy_mode.lower() == 'incremental':
                copy_mode = 'INCREMENTAL'
            else:
                copy_mode = 'FULL'

            # DBXSCDType: 1 = SCD Type 1, 2 = SCD Type 2 (default 1 unless Lookup detected)
            dbx_scd_type = 2 if has_lookup_scd2 else 1
            # DBXPipelineId: identifier for Databricks pipeline/notebook (e.g. task name)
            dbx_pipeline_id = (task_name or '').replace("'", "''")
            # JobId: logical grouping (use 1 or package-based)
            job_id = 1
            # ADLSContainerName: placeholder for Bronze container
            adls_container = "bronze"
            # DeltaColumnName, WaterMark: infer from query if possible
            delta_col = ''
            watermark_name = ''
            watermark_type = ''
            if source_query:
                watermark_name, watermark_type = _infer_watermark_from_query(source_query)
                if watermark_name:
                    delta_col = watermark_name
            # IsActive: 1
            is_active = 1
            # FolderPath: e.g. bronze/schema/tablename
            folder_path = f"bronze/{src_schema or 'dbo'}/{src_table or 'table'}".replace("'", "''")
            # Partition and optional columns: NULL
            part_col = ''
            part_lower = ''
            part_upper = ''
            masking = ''
            ind_field = ''
            ind_value = ''

            def nv(s: str, max_len: Optional[int] = None) -> str:
                if s is None or (isinstance(s, str) and not s.strip()):
                    return "NULL"
                return _escape_sql_nvarchar(s, max_len)

            row_values = (
                f"({nv(adf_job_name)}, {nv(src_schema)}, {nv(src_table)}, {nv(src_id)}, {nv(source_query)}, "
                f"{nv(tgt_table)}, {nv(tgt_schema)}, {nv(tgt_db)}, {dbx_scd_type}, {nv(dbx_pipeline_id)}, "
                f"{job_id}, {nv(adls_container)}, N'{copy_mode}', {nv(delta_col)}, {is_active}, "
                f"{nv(watermark_name)}, {nv(watermark_type)}, {nv(folder_path)}, {nv(part_col)}, "
                f"{nv(part_lower)}, {nv(part_upper)}, {nv(masking)}, {nv(ind_field)}, {nv(ind_value)})"
            )
            insert_statements.append(
                "INSERT INTO dbo.ControlTableIntegrated (\n  " + col_list_str + "\n) VALUES\n  " + row_values + ";"
            )

    if not insert_statements:
        return "\n".join(header_lines + ["-- No source/destination pairs found in Data Flow tasks"])
    return "\n\n".join(header_lines + insert_statements)


def generate_pyspark_notebook(parsed_data: Dict[str, Any], data_flow_task_name: Optional[str] = None) -> Dict[str, str]:
    """
    Generate PySpark notebooks for Databricks (Silver/Gold) from Data Flow tasks.
    Returns dict: notebook_name -> notebook_content (source).
    """
    notebooks = {}
    metadata = parsed_data.get('metadata', {})
    package_name = (metadata.get('packageName') or metadata.get('objectName') or 'SSISPackage').replace(' ', '_')
    
    for activity in parsed_data.get('activities', []):
        if activity.get('type') != 'Data Flow Task':
            continue
        task_name = activity.get('name', '')
        if data_flow_task_name and task_name != data_flow_task_name:
            continue
        components = activity.get('components', [])
        if not components:
            continue
        
        cells = []
        cells.append({
            "cell_type": "markdown",
            "source": [f"# {task_name}\n\nGenerated from SSIS package: {package_name}\n\nSilver/Gold transformation."]
        })
        cells.append({
            "cell_type": "code",
            "source": ["from pyspark.sql import SparkSession\nfrom pyspark.sql import functions as F\nfrom pyspark.sql.types import *\n\n# spark already available in Databricks"]
        })
        source_comp = next((c for c in components if c.get('componentType') == 'Source'), None)
        dest_comp = next((c for c in components if c.get('componentType') == 'Destination'), None)
        source_code = "df = spark.table('bronze.placeholder')  # Replace with your source table or spark.read.jdbc(...)"
        if source_comp and source_comp.get('sourceMetadata'):
            sm = source_comp['sourceMetadata']
            schema, table = sm.get('sourceSchemaName', ''), sm.get('sourceTableName', '')
            if schema:
                source_code = f"# Source: {schema}.{table}\ndf = spark.table('{schema}.{table}')"
            else:
                source_code = f"# Source: {table}\ndf = spark.table('{table}')"
            if sm.get('sourceQuery'):
                q = (sm.get('sourceQuery', '') or '').replace('"""', "'")
                source_code = f"# Source query\nquery = \"\"\"{q}\"\"\"\ndf = spark.read.jdbc(url=..., table=f\"({{query}}) t\")  # Configure JDBC"
        cells.append({"cell_type": "code", "source": [source_code]})
        
        trans_code = []
        for comp in components:
            if comp.get('componentType') != 'Transformation':
                continue
            logic = comp.get('transformationLogic', {})
            pyspark_eq = logic.get('pysparkEquivalent')
            if pyspark_eq == 'withColumn' and logic.get('expressions'):
                for e in logic['expressions']:
                    expr = (e.get('friendlyExpression') or e.get('expression') or '').strip()
                    if expr:
                        col_name = e.get('outputColumn', '')
                        trans_code.append(f"df = df.withColumn('{col_name}', F.expr(\"{expr}\"))  # Derived Column")
            elif pyspark_eq == 'cast()':
                for col in comp.get('outputColumns', []):
                    trans_code.append(f"# df = df.withColumn('{col.get('name')}', F.col('{col.get('name')}').cast(...))")
            elif pyspark_eq == 'orderBy':
                trans_code.append("# df = df.orderBy(...)")
            elif pyspark_eq == 'groupBy':
                trans_code.append("# df = df.groupBy(...).agg(...)")
            elif pyspark_eq == 'when/otherwise':
                trans_code.append("# df = df.withColumn(..., F.when(...).otherwise(...))")
        if trans_code:
            cells.append({"cell_type": "code", "source": ["\n".join(trans_code)]})
        
        target_table = "silver.placeholder"
        if dest_comp and dest_comp.get('destinationMetadata'):
            dm = dest_comp['destinationMetadata']
            s, t = dm.get('targetSchemaName', ''), dm.get('targetTableName', '')
            target_table = f"{s}.{t}" if s else t
        write_code = f"# Write to Silver/Gold (Delta)\ndf.write.format('delta').mode('overwrite').saveAsTable('{target_table}')"
        cells.append({"cell_type": "code", "source": [write_code]})
        
        notebook_name = f"{package_name}_{task_name.replace(' ', '_')}.py"
        source_lines = []
        for c in cells:
            if c["cell_type"] == "markdown":
                source_lines.append("# MAGIC %md")
                source_lines.extend((c["source"][0] or "").split("\n"))
            else:
                source_lines.extend((c["source"][0] or "").split("\n"))
            source_lines.append("")
        notebooks[notebook_name] = "\n".join(source_lines)
    
    return notebooks


def resolve_connection_details(connection_id: str, connection_managers: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Resolve connection details from connection ID (GUID or refId)."""
    if not connection_id:
        return None
    
    # Normalize the connection ID (remove braces, trim whitespace, handle :invalid suffix)
    clean_id = connection_id.strip('{}').strip()
    # Remove :invalid suffix if present (e.g., "{GUID}:invalid")
    if ':invalid' in clean_id:
        clean_id = clean_id.split(':invalid')[0].strip()
    
    for conn in connection_managers:
        # Check by DTSID (GUID format like {9B379199-C6FE-4718-ADA8-2A22CE604343})
        conn_dtsid = conn.get('dtsId', '')
        if conn_dtsid:
            clean_dtsid = conn_dtsid.strip('{}').strip()
            if clean_dtsid.lower() == clean_id.lower():
                return conn
        
        # Check by refId (e.g., "Package.ConnectionManagers[USIDCVSQL0252.FA_RADCOM.FA_DATA_DBO]")
        conn_refid = conn.get('id', '')
        if conn_refid:
            # Try exact match first
            if conn_refid.lower() == connection_id.lower():
                return conn
            # Try normalized match
            if conn_refid.lower() == clean_id.lower():
                return conn
            
            # Extract and normalize connection names from both refId and connection_id
            # Format: Package.ConnectionManagers[NAME]
            def extract_conn_name(refid_str: str) -> str:
                """Extract connection name from refId format and normalize it."""
                if not refid_str:
                    return ''
                if '[' in refid_str and ']' in refid_str:
                    name = refid_str.split('[')[1].split(']')[0]
                    # Normalize: remove double dots, replace multiple consecutive dots with single dot
                    name = re.sub(r'\.{2,}', '.', name)  # Replace 2+ dots with single dot
                    # Remove any trailing/leading dots
                    name = name.strip('.')
                    return name.lower()
                return refid_str.lower()
            
            def normalize_refid(refid_str: str) -> str:
                """Normalize refId by extracting and normalizing the connection name."""
                if not refid_str:
                    return ''
                if '[' in refid_str and ']' in refid_str:
                    prefix = refid_str.split('[')[0]  # "Package.ConnectionManagers"
                    name = extract_conn_name(refid_str)
                    return f"{prefix}[{name}]"
                return refid_str.lower()
            
            # Extract connection names and normalize them
            conn_name = extract_conn_name(conn_refid)
            connection_id_name = extract_conn_name(connection_id)
            
            # Match by normalized connection name (handles variations like double dots)
            # This is the primary matching method for refId-based connections
            if conn_name and connection_id_name:
                # Direct match after normalization
                if conn_name == connection_id_name:
                    return conn
                # Also try removing all dots and comparing (for edge cases)
                conn_name_no_dots = conn_name.replace('.', '')
                conn_id_name_no_dots = connection_id_name.replace('.', '')
                if conn_name_no_dots and conn_id_name_no_dots and conn_name_no_dots == conn_id_name_no_dots:
                    return conn
            
            # Try normalized refId matching
            normalized_conn_refid = normalize_refid(conn_refid)
            normalized_connection_id = normalize_refid(connection_id)
            
            if normalized_conn_refid and normalized_connection_id:
                if normalized_conn_refid == normalized_connection_id:
                    return conn
            
            # Fallback: check if normalized names are similar (fuzzy match)
            if conn_name and connection_id_name:
                # Check if one name contains the other (handles partial matches)
                if conn_name in connection_id_name or connection_id_name in conn_name:
                    return conn
                # Also try removing common prefixes and comparing
                # Extract just the server/database part for comparison
                conn_parts = conn_name.split('.')
                conn_id_parts = connection_id_name.split('.')
                if len(conn_parts) > 0 and len(conn_id_parts) > 0:
                    # Compare last parts (database name) and first parts (server)
                    if conn_parts[-1] == conn_id_parts[-1] and conn_parts[0] == conn_id_parts[0]:
                        return conn
            
            # Also check if clean_id is in refId or vice versa (substring match)
            if clean_id.lower() in conn_refid.lower() or conn_refid.lower() in clean_id.lower():
                return conn
    
    return None


def build_connections_usage_map(activities: List[Dict[str, Any]], connection_managers: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    FR-2+: Build a consolidated map of all connections used in the package.
    Shows where each connection is used (SQL tasks, components, etc.) with full details.
    
    Returns: {
        'allConnections': [List of all connections with usage info],
        'connectionUsageMap': {connection_id/name -> List of usages},
        'unusedConnections': [List of defined but unused connections]
    }
    """
    # Track which connections are actually used
    used_connection_ids = set()
    connection_usages = {}  # connection_id -> list of usage info
    
    # Build detailed usage map
    for activity in activities:
        # Activity-level connections (SQL tasks)
        if activity.get('connectionId'):
            conn_id = activity['connectionId']
            used_connection_ids.add(conn_id)
            if conn_id not in connection_usages:
                connection_usages[conn_id] = []
            connection_usages[conn_id].append({
                'usedIn': 'Activity',
                'activityName': activity.get('name', ''),
                'activityType': activity.get('type', ''),
                'activityId': activity.get('id', ''),
                'locationInPackage': f"Activity: {activity.get('name', '')}"
            })
        
        # Component-level connections (Data Flow transformations)
        if activity.get('components'):
            for component in activity['components']:
                # Handle both old (connectionId) and new (componentConnections) formats
                if component.get('connectionId'):
                    conn_id = component['connectionId']
                    used_connection_ids.add(conn_id)
                    if conn_id not in connection_usages:
                        connection_usages[conn_id] = []
                    connection_usages[conn_id].append({
                        'usedIn': 'Component',
                        'componentName': component.get('name', ''),
                        'componentType': component.get('componentTypeName', component.get('componentType', '')),
                        'componentId': component.get('id', ''),
                        'activityName': activity.get('name', ''),
                        'locationInPackage': f"Data Flow > {component.get('name', '')} ({component.get('componentTypeName', '')})"
                    })
                
                # New: Handle multiple component connections
                if component.get('componentConnections'):
                    for comp_conn in component['componentConnections']:
                        conn_id = comp_conn.get('connectionManagerRefId') or comp_conn.get('connectionManagerID')
                        if conn_id:
                            used_connection_ids.add(conn_id)
                            if conn_id not in connection_usages:
                                connection_usages[conn_id] = []
                            connection_usages[conn_id].append({
                                'usedIn': 'Component',
                                'componentName': component.get('name', ''),
                                'componentType': component.get('componentTypeName', component.get('componentType', '')),
                                'componentId': component.get('id', ''),
                                'connectionName': comp_conn.get('name', ''),
                                'connectionDescription': comp_conn.get('description', ''),
                                'activityName': activity.get('name', ''),
                                'locationInPackage': f"Data Flow > {component.get('name', '')} > {comp_conn.get('name', '')} ({component.get('componentTypeName', '')})"
                            })
    
    # Build enhanced connection list with usage information
    all_connections_with_usage = []
    for conn in connection_managers:
        conn_id = conn.get('id', '')
        conn_name = conn.get('name', '')
        
        usage_list = connection_usages.get(conn_id, [])
        
        enhanced_conn = {
            **conn,  # Include all original connection info
            'usageCount': len(usage_list),
            'usedInActivities': len([u for u in usage_list if u['usedIn'] == 'Activity']),
            'usedInComponents': len([u for u in usage_list if u['usedIn'] == 'Component']),
            'usageDetails': usage_list,
        }
        all_connections_with_usage.append(enhanced_conn)
    
    # Find unused connections
    unused_connections = [
        conn for conn in all_connections_with_usage 
        if conn.get('usageCount', 0) == 0
    ]
    
    return {
        'allConnections': all_connections_with_usage,
        'connectionUsageMap': connection_usages,
        'unusedConnections': unused_connections,
        'totalConnections': len(connection_managers),
        'usedConnections': len([c for c in all_connections_with_usage if c.get('usageCount', 0) > 0]),
        'unusedConnectionCount': len(unused_connections),
    }


def parse_package_metadata(root) -> Dict[str, Any]:
    """FR-2: Extract package-level metadata (Package Name, etc.)."""
    object_name = extract_attribute(root, 'ObjectName')
    metadata = {
        'name': extract_attribute(root, 'refId', 'Package'),
        'packageName': object_name or extract_attribute(root, 'refId', 'Package'),
        'objectName': object_name,
        'creationDate': extract_attribute(root, 'CreationDate'),
        'creator': extract_attribute(root, 'CreatorName'),
        'creatorComputer': extract_attribute(root, 'CreatorComputerName'),
        'dtsId': extract_attribute(root, 'DTSID'),
        'versionBuild': extract_attribute(root, 'VersionBuild'),
        'versionGuid': extract_attribute(root, 'VersionGUID')
    }
    
    # Get description if available
    description = extract_attribute(root, 'Description')
    if description:
        metadata['description'] = description
    
    return metadata


def format_xml_pretty(xml_content: bytes) -> str:
    """
    Format XML with proper indentation (like Visual Studio View Code).
    Preserves the original XML structure while adding proper indentation.
    """
    try:
        # Decode to string first
        xml_string = xml_content.decode('utf-8')
    except:
        try:
            xml_string = xml_content.decode('utf-8', errors='replace')
        except:
            return xml_content.decode('utf-8', errors='replace')
    
    # Use lxml to format XML (like Visual Studio does)
    try:
        # Parse XML with parser that preserves structure
        parser = etree.XMLParser(
            strip_cdata=False,
            remove_blank_text=False,
            resolve_entities=False
        )
        root = etree.fromstring(xml_content, parser=parser)
        
        # Format with pretty printing (2-space indentation like Visual Studio)
        formatted_bytes = etree.tostring(
            root,
            pretty_print=True,
            xml_declaration=True,
            encoding='utf-8',
            method='xml'
        )
        
        formatted = formatted_bytes.decode('utf-8')
        
        # Post-process to match Visual Studio's formatting style
        # Remove excessive blank lines but keep structure
        lines = formatted.split('\n')
        cleaned_lines = []
        prev_empty = False
        
        for line in lines:
            is_empty = not line.strip()
            if is_empty:
                # Only keep single blank lines between major sections
                if not prev_empty and len(cleaned_lines) > 0:
                    # Check if previous line was a closing tag or next line is opening
                    if cleaned_lines and cleaned_lines[-1].strip().startswith('</'):
                        cleaned_lines.append('')
                prev_empty = True
            else:
                cleaned_lines.append(line)
                prev_empty = False
        
        # Join and ensure consistent line endings
        result = '\n'.join(cleaned_lines)
        
        # Ensure XML declaration is on first line if present
        if result.startswith('<?xml'):
            lines = result.split('\n', 1)
            if len(lines) > 1 and lines[1].strip():
                result = lines[0] + '\n' + lines[1]
        
        return result
    except Exception as e:
        # If parsing fails, try simple string-based formatting
        return format_xml_string_based(xml_string)


def format_xml_string_based(xml_string: str) -> str:
    """
    Simple string-based XML formatter as fallback.
    Adds indentation based on tag depth.
    """
    import re
    
    # Remove existing indentation
    xml_string = xml_string.strip()
    
    # Add line breaks between tags
    xml_string = re.sub(r'><', '>\n<', xml_string)
    
    # Split into lines
    lines = xml_string.split('\n')
    formatted_lines = []
    indent_level = 0
    indent_size = 2
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Check if this is a closing tag
        if line.startswith('</'):
            indent_level = max(0, indent_level - 1)
            formatted_lines.append(' ' * (indent_level * indent_size) + line)
        # Check if this is a self-closing tag
        elif line.endswith('/>'):
            formatted_lines.append(' ' * (indent_level * indent_size) + line)
        # Check if this is an opening tag
        elif line.startswith('<') and not line.startswith('</'):
            formatted_lines.append(' ' * (indent_level * indent_size) + line)
            # Only increase indent if it's not a self-closing tag
            if not line.endswith('/>') and not line.startswith('<?') and not line.startswith('<!--'):
                indent_level += 1
        else:
            # Text content
            formatted_lines.append(' ' * (indent_level * indent_size) + line)
    
    return '\n'.join(formatted_lines)


def is_password_protected(content: bytes) -> bool:
    """
    Detect if an SSIS package is password-protected.
    Password-protected packages typically have specific markers or fail to parse.
    """
    try:
        # Try to decode and check for password protection indicators
        xml_string = content.decode('utf-8', errors='ignore')
        
        # Check for common password protection indicators
        # SSIS packages with password protection may have specific attributes
        if 'ProtectionLevel' in xml_string:
            # Check if ProtectionLevel indicates password protection
            # ProtectionLevel="2" or "3" typically means password-protected
            import re
            protection_match = re.search(r'ProtectionLevel\s*=\s*["\']?([0-9])["\']?', xml_string, re.IGNORECASE)
            if protection_match:
                level = protection_match.group(1)
                # Level 2 = EncryptSensitiveWithPassword, Level 3 = EncryptAllWithPassword
                if level in ['2', '3']:
                    return True
        
        # Try to parse - if it fails with specific errors, might be password-protected
        try:
            etree.fromstring(content)
        except etree.XMLSyntaxError:
            # If XML is malformed, might be encrypted/password-protected
            # Check if it looks like encrypted content
            if b'EncryptedData' in content or b'encrypted' in content.lower():
                return True
        
        return False
    except Exception:
        # If we can't determine, assume not password-protected
        return False


def decrypt_dtsx_with_password(content: bytes, password: str, filename: str) -> bytes:
    """
    Decrypt a password-protected SSIS package using dtutil.exe or COM automation.
    Returns decrypted XML content.
    """
    if platform.system() != 'Windows':
        raise HTTPException(
            status_code=400,
            detail="Password-protected SSIS packages can only be decrypted on Windows systems."
        )
    
    # Method 1: Try using dtutil.exe (SQL Server Integration Services utility)
    dtutil_paths = [
        r"C:\Program Files\Microsoft SQL Server\150\DTS\Binn\dtutil.exe",  # SQL Server 2019
        r"C:\Program Files\Microsoft SQL Server\140\DTS\Binn\dtutil.exe",  # SQL Server 2017
        r"C:\Program Files\Microsoft SQL Server\130\DTS\Binn\dtutil.exe",  # SQL Server 2016
        r"C:\Program Files (x86)\Microsoft SQL Server\150\DTS\Binn\dtutil.exe",
        r"C:\Program Files (x86)\Microsoft SQL Server\140\DTS\Binn\dtutil.exe",
        r"C:\Program Files (x86)\Microsoft SQL Server\130\DTS\Binn\dtutil.exe",
    ]
    
    dtutil_exe = None
    for path in dtutil_paths:
        if os.path.exists(path):
            dtutil_exe = path
            break
    
    if dtutil_exe:
        try:
            # Create temporary files
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.dtsx', delete=False) as temp_input:
                temp_input.write(content)
                temp_input_path = temp_input.name
            
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.dtsx', delete=False) as temp_output:
                temp_output_path = temp_output.name
            
            try:
                # Use dtutil to decrypt the package
                # dtutil /File input.dtsx /Decrypt password /CopyFileTo output.dtsx
                cmd = [
                    dtutil_exe,
                    '/File', temp_input_path,
                    '/Decrypt', password,
                    '/CopyFileTo', temp_output_path
                ]
                
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if result.returncode == 0 and os.path.exists(temp_output_path):
                    # Read decrypted content
                    with open(temp_output_path, 'rb') as f:
                        decrypted_content = f.read()
                    return decrypted_content
                else:
                    # Combine stderr and stdout for error detection
                    error_msg = (result.stderr or "") + "\n" + (result.stdout or "")
                    if not error_msg.strip():
                        error_msg = "Unknown error"
                    
                    # Check if error is about missing SSIS components
                    error_lower = error_msg.lower()
                    is_ssis_installation_error = any(keyword in error_lower for keyword in [
                        "integration services", "database engine", "requires integration",
                        "this application requires", "to be installed", "sql server setup",
                        "install a component", "run sql server setup", "sql server 2016",
                        "standard, enterprise, developer, business intelligence, or evaluation"
                    ])
                    
                    if is_ssis_installation_error:
                        # dtutil exists but SSIS components aren't installed
                        # Fall through to try COM automation method instead
                        pass
                    else:
                        # Other dtutil errors (wrong password, corrupted file, etc.)
                        # Clean up error message (use stderr if available, otherwise stdout)
                        clean_error = result.stderr.strip() if result.stderr and result.stderr.strip() else (result.stdout.strip() if result.stdout and result.stdout.strip() else error_msg)
                        raise HTTPException(
                            status_code=400,
                            detail=f"Failed to decrypt package: {clean_error}"
                        )
            finally:
                # Clean up temp files
                try:
                    os.unlink(temp_input_path)
                except:
                    pass
                try:
                    os.unlink(temp_output_path)
                except:
                    pass
        except subprocess.TimeoutExpired:
            # If dtutil timed out, try COM automation as fallback
            pass
        except HTTPException:
            # Re-raise HTTP exceptions (wrong password, etc.)
            raise
        except Exception as e:
            # If dtutil failed for other reasons, try COM automation as fallback
            error_str = str(e).lower()
            if "integration services" not in error_str and "database engine" not in error_str:
                # Only fall through if it's not an installation error
                pass
    
    # Method 2: Try using COM automation (Windows only, requires pywin32)
    # This is used as primary method if dtutil not found, or as fallback if dtutil fails
    try:
        try:
            import win32com.client
            import pythoncom
        except ImportError:
            # If dtutil also failed or wasn't found, provide helpful error message
            if dtutil_exe:
                raise HTTPException(
                    status_code=400,
                    detail="Failed to decrypt package: SQL Server Integration Services components are not properly installed, and pywin32 is required as an alternative. Please install pywin32 with: pip install pywin32, or install SQL Server Integration Services."
                )
            else:
                raise HTTPException(
                    status_code=500,
                    detail="pywin32 is required for password-protected packages. Install it with: pip install pywin32"
                )
            
            # Initialize COM
            pythoncom.CoInitialize()
            
            try:
                # Create DTS Application object
                dts_app = win32com.client.Dispatch("DTS.Application")
                
                # Load package from memory (we'll need to save to temp file first)
                with tempfile.NamedTemporaryFile(mode='wb', suffix='.dtsx', delete=False) as temp_file:
                    temp_file.write(content)
                    temp_file_path = temp_file.name
                
                try:
                    # Load package
                    package = dts_app.LoadPackage(temp_file_path, None)
                    
                    # Set password if needed
                    if password:
                        package.Password = password
                    
                    # Save to XML format (this decrypts if password is correct)
                    with tempfile.NamedTemporaryFile(mode='wb', suffix='.xml', delete=False) as temp_output:
                        temp_output_path = temp_output.name
                    
                    try:
                        package.SaveToXML(temp_output_path)
                        
                        # Read decrypted content
                        with open(temp_output_path, 'rb') as f:
                            decrypted_content = f.read()
                        
                        return decrypted_content
                    finally:
                        try:
                            os.unlink(temp_output_path)
                        except:
                            pass
                finally:
                    try:
                        os.unlink(temp_file_path)
                    except:
                        pass
            finally:
                pythoncom.CoUninitialize()
    except HTTPException:
        raise  # Re-raise HTTP exceptions
    except Exception as e:
        error_msg = str(e)
        error_lower = error_msg.lower()
        
        # Check for password-related errors
        if "password" in error_lower or "incorrect" in error_lower or "wrong" in error_lower:
            raise HTTPException(
                status_code=400,
                detail="Incorrect password or package is not password-protected."
            )
        
        # Check for missing COM components
        if "dts.application" in error_lower or "com" in error_lower or "dispatch" in error_lower:
            if dtutil_exe:
                raise HTTPException(
                    status_code=400,
                    detail="Failed to decrypt package: SQL Server Integration Services components are not properly installed. Please install SQL Server Integration Services or ensure dtutil.exe can access the required components."
                )
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Failed to decrypt package: {error_msg}. Please ensure SQL Server Integration Services is installed, or install pywin32 for COM automation support."
                )
        
        # Generic error
        raise HTTPException(
            status_code=400,
            detail=f"Failed to decrypt package: {error_msg}"
        )
    
    # If all methods fail (shouldn't reach here, but keep as safety)
    raise HTTPException(
        status_code=400,
        detail="Could not decrypt package. dtutil.exe not found and COM automation failed. Please install SQL Server Integration Services or pywin32."
    )


def convert_dtsx_to_xml(content: bytes, filename: str) -> bytes:
    """
    Convert DTSX file to XML format (like Visual Studio does).
    DTSX files are XML files, but may have encoding issues.
    This function handles encoding detection and conversion.
    """
    # Check if file has BOM (Byte Order Mark) for UTF-16
    if content.startswith(b'\xff\xfe'):
        # UTF-16 LE (Little Endian) - most common Windows encoding
        try:
            decoded = content.decode('utf-16-le')
            # Re-encode as UTF-8 for consistent processing
            return decoded.encode('utf-8')
        except UnicodeDecodeError:
            pass
    elif content.startswith(b'\xfe\xff'):
        # UTF-16 BE (Big Endian)
        try:
            decoded = content.decode('utf-16-be')
            return decoded.encode('utf-8')
        except UnicodeDecodeError:
            pass
    elif content.startswith(b'\xef\xbb\xbf'):
        # UTF-8 with BOM - remove BOM and return
        return content[3:]
    
    # Try UTF-8 first (most common for modern files)
    try:
        decoded = content.decode('utf-8')
        return content  # Already UTF-8, return as-is
    except UnicodeDecodeError:
        pass
    
    # Try UTF-16 without BOM (common for older SSIS packages)
    try:
        decoded = content.decode('utf-16-le')
        return decoded.encode('utf-8')
    except UnicodeDecodeError:
        pass
    
    # Try Windows-1252 (common for older Windows files)
    try:
        decoded = content.decode('windows-1252')
        return decoded.encode('utf-8')
    except UnicodeDecodeError:
        pass
    
    # Last resort: try UTF-8 with error replacement
    try:
        decoded = content.decode('utf-8', errors='replace')
        return decoded.encode('utf-8')
    except Exception:
        # If all else fails, return original content
        return content


def calculate_component_summary(activities: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate component summary: count of activity types and data flow component types.
    Returns a dictionary with activityTypeCounts and dataFlowComponentTypeCounts.
    """
    # Count activity types
    activity_type_counts: Dict[str, int] = {}
    data_flow_component_type_counts: Dict[str, int] = {}
    
    for activity in activities:
        activity_type = activity.get('type', 'Unknown')
        activity_type_counts[activity_type] = activity_type_counts.get(activity_type, 0) + 1
        
        # Count data flow components within Data Flow Tasks
        if activity.get('components'):
            for component in activity['components']:
                component_type = component.get('componentType', 'Unknown')
                component_type_name = component.get('componentTypeName', component_type)
                # Use componentTypeName if available, otherwise use componentType
                display_name = component_type_name if component_type_name else component_type
                data_flow_component_type_counts[display_name] = data_flow_component_type_counts.get(display_name, 0) + 1
    
    # Convert to list format for easier frontend consumption
    activity_summary = [
        {'name': name, 'count': count}
        for name, count in sorted(activity_type_counts.items(), key=lambda x: (-x[1], x[0]))
    ]
    
    data_flow_component_summary = [
        {'name': name, 'count': count}
        for name, count in sorted(data_flow_component_type_counts.items(), key=lambda x: (-x[1], x[0]))
    ]
    
    return {
        'activityTypeCounts': activity_summary,
        'dataFlowComponentTypeCounts': data_flow_component_summary,
        'totalActivities': len(activities),
        'totalDataFlowComponents': sum(data_flow_component_type_counts.values()),
    }


@app.post("/api/parse-dtsx")
async def parse_dtsx(
    file: UploadFile = File(...)
):
    """
    Parse SSIS XML file and extract workflow information.
    Only supports .xml file format.
    """
    try:
        # Read file content
        content = await file.read()
        filename = file.filename or ''
        file_extension = filename.lower()
        
        # Accept SSIS package (.dtsx or .xml)
        if not (file_extension.endswith('.xml') or file_extension.endswith('.dtsx')):
            raise HTTPException(
                status_code=400,
                detail="Invalid file type. Please upload an SSIS package (.dtsx or .xml file)."
            )
        
        # Parse XML
        try:
            root = etree.fromstring(content)
        except etree.XMLSyntaxError as e:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid XML file: {str(e)}. Please ensure the file is a valid SSIS package XML."
            )
        
        # FR-2/FR-3/FR-4: Extract all components (Control Flow + Data Flow, Variables, Execution sequence)
        metadata = parse_package_metadata(root)
        connection_managers = parse_connection_managers(root)
        variables = parse_variables(root)
        activities = parse_activities(root, variables=variables)
        precedence_map, constraints_detail = parse_precedence_constraints_detailed(root)
        execution_sequence = build_execution_sequence(activities, precedence_map)
        
        # Step 4 & 5: First activities (never in To), Last activities (never in From)
        all_ref_ids = {a['id'].strip() for a in activities}
        all_to_ref_ids = {to_id.strip() for to_id in precedence_map.keys()}
        all_from_ref_ids = set()
        for from_list in precedence_map.values():
            for f in from_list:
                all_from_ref_ids.add(f.strip())
        first_activities_ref_ids = all_ref_ids - all_to_ref_ids
        last_activities_ref_ids = all_ref_ids - all_from_ref_ids
        first_activities = [{'refId': rid, 'name': next((a['name'] for a in activities if a['id'].strip() == rid), rid)} for rid in first_activities_ref_ids]
        last_activities = [{'refId': rid, 'name': next((a['name'] for a in activities if a['id'].strip() == rid), rid)} for rid in last_activities_ref_ids]
        
        # Constraint (From,To) -> value for condition on edges
        constraint_value_map: Dict[tuple, str] = {}
        for c in constraints_detail:
            constraint_value_map[(c['fromRefId'].strip(), c['toRefId'].strip())] = c.get('value', 'Success')
        
        # Build activity lookup by refId - normalize refIds for matching
        # Create multiple lookup strategies: exact, case-insensitive, and normalized
        activity_by_refid = {}
        activity_by_refid_lower = {}  # Case-insensitive lookup
        for activity in activities:
            refid = activity['id'].strip()
            activity_by_refid[refid] = activity
            activity_by_refid_lower[refid.lower()] = activity
        
        def find_activity_by_id(refid: str):
            """Find activity by refId with multiple matching strategies."""
            refid_normalized = refid.strip()
            # Try exact match first
            if refid_normalized in activity_by_refid:
                return activity_by_refid[refid_normalized]
            # Try case-insensitive match
            if refid_normalized.lower() in activity_by_refid_lower:
                return activity_by_refid_lower[refid_normalized.lower()]
            return None
        
        # Build reverse precedence map to find next activities (activities that depend on current one)
        next_activities_map: Dict[str, List[str]] = {}
        for to_activity, from_activities in precedence_map.items():
            for from_activity in from_activities:
                from_activity_normalized = from_activity.strip()
                to_activity_normalized = to_activity.strip()
                if from_activity_normalized not in next_activities_map:
                    next_activities_map[from_activity_normalized] = []
                # Avoid duplicates
                if to_activity_normalized not in next_activities_map[from_activity_normalized]:
                    next_activities_map[from_activity_normalized].append(to_activity_normalized)
        
        # Also create case-insensitive version of next_activities_map for matching
        next_activities_map_lower: Dict[str, List[str]] = {}
        for key, values in next_activities_map.items():
            next_activities_map_lower[key.lower()] = values
        
        # Add previous and next activity information to each activity
        for activity in activities:
            previous_activities = []
            next_activities = []
            activity_refid = activity['id'].strip()
            
            # Step 6: Previous activities (with condition from precedence constraint)
            if activity_refid in precedence_map:
                for prev_refid in precedence_map[activity_refid]:
                    prev_refid_norm = prev_refid.strip()
                    condition = constraint_value_map.get((prev_refid_norm, activity_refid), 'Success')
                    prev_activity = find_activity_by_id(prev_refid)
                    if prev_activity:
                        previous_activities.append({
                            'id': prev_activity['id'],
                            'name': prev_activity['name'],
                            'condition': condition,
                        })
            
            # Check reverse map for next activities (activities that depend on this one)
            # Try multiple matching strategies
            next_refids_to_check = []
            
            # Strategy 1: Exact match
            if activity_refid in next_activities_map:
                next_refids_to_check = next_activities_map[activity_refid]
            # Strategy 2: Case-insensitive match
            elif activity_refid.lower() in next_activities_map_lower:
                next_refids_to_check = next_activities_map_lower[activity_refid.lower()]
            # Strategy 3: Try iterating through all keys to find a match (handles any format differences)
            else:
                for map_key, map_values in next_activities_map.items():
                    # Try exact match on normalized versions
                    if activity_refid.strip() == map_key.strip():
                        next_refids_to_check = map_values
                        break
                    # Try case-insensitive match
                    elif activity_refid.strip().lower() == map_key.strip().lower():
                        next_refids_to_check = map_values
                        break
            
            # Step 6: Next activities (with condition)
            for next_refid in next_refids_to_check:
                next_refid_norm = next_refid.strip()
                condition = constraint_value_map.get((activity_refid, next_refid_norm), 'Success')
                next_activity = find_activity_by_id(next_refid)
                if next_activity:
                    if not any(na['id'] == next_activity['id'] for na in next_activities):
                        next_activities.append({
                            'id': next_activity['id'],
                            'name': next_activity['name'],
                            'condition': condition,
                        })
            
            activity['previousActivities'] = previous_activities
            activity['nextActivities'] = next_activities
            # Step 7: Parallel = true when this activity has multiple outgoing constraints
            activity['parallel'] = len(next_activities) > 1
            # Step 6 output: previous (names), next (names), condition (primary condition if single)
            activity['previous'] = [p['name'] for p in previous_activities]
            activity['next'] = [n['name'] for n in next_activities]
            activity['condition'] = previous_activities[0]['condition'] if len(previous_activities) == 1 else (next_activities[0]['condition'] if next_activities else 'Success')
            # Step 8: Incoming logical operator (AND = all previous must succeed; OR = any)
            incoming_constraints = [c for c in constraints_detail if c['toRefId'].strip() == activity_refid]
            activity['incomingLogicalAnd'] = all(c.get('logicalAnd', True) for c in incoming_constraints) if incoming_constraints else True
        
        # Resolve connection details for each activity and its components
        for activity in activities:
            connection_details = None
            
            # Check if activity has direct connectionId (SQL tasks)
            if activity.get('connectionId'):
                connection_details = resolve_connection_details(activity['connectionId'], connection_managers)
            
            # Resolve connection details for Data Flow components (Source and Destination)
            if activity.get('components'):
                for component in activity['components']:
                    if component.get('connectionId'):
                        component_conn_details = resolve_connection_details(component['connectionId'], connection_managers)
                        if component_conn_details:
                            component['connectionDetails'] = component_conn_details
                            # FR-3: Set TargetDBName from connection initialCatalog for destinations
                            if component.get('destinationMetadata'):
                                component['destinationMetadata']['targetDBName'] = component_conn_details.get('initialCatalog', '') or component_conn_details.get('dataSource', '')
                        if not connection_details:
                            connection_details = component_conn_details
            
            # Add connection details to activity
            if connection_details:
                activity['connectionDetails'] = connection_details
        
        # Format XML for display
        formatted_xml = format_xml_pretty(content)
        
        # Step 9: Containers = Sequence Containers with nested activity refIds
        containers = []
        for act in activities:
            if act.get('type') == 'Sequence Container':
                container_ref_id = act['id'].strip()
                nested_ref_ids = [a['id'].strip() for a in activities if a.get('parentContainerRefId', '').strip() == container_ref_id]
                containers.append({
                    'containerRefId': container_ref_id,
                    'containerName': act.get('name', ''),
                    'activityRefIds': nested_ref_ids,
                    'activities': [{'refId': a['id'], 'name': a['name'], 'type': a.get('type', '')} for a in activities if a.get('parentContainerRefId', '').strip() == container_ref_id],
                })
        
        # Step 3 & 10: Activity flow graph (DAG: first, last, constraints; Data Flow paths on tasks)
        activity_flow_graph = {
            'firstActivities': first_activities,
            'lastActivities': last_activities,
            'constraintsDetail': constraints_detail,
            'precedenceMap': precedence_map,
        }
        
        # Calculate component summary (activity types and data flow component types)
        component_summary = calculate_component_summary(activities)
        
        # FR-2+: Build connections usage map to show where each connection is used
        connections_usage_map = build_connections_usage_map(activities, connection_managers)
        
        # Package-wide referenced tables (Execute SQL, Data Flow sources/destinations/transformations)
        pkg_ref_tables = aggregate_package_referenced_tables(activities)

        # Build response (FR-2/FR-3/FR-4: execution sequence, variables, activity flow)
        parsed_data = {
            'metadata': metadata,
            'connectionManagers': connection_managers,
            'connectionsUsageMap': connections_usage_map,
            'variables': variables,
            'activities': activities,
            'executionSequence': execution_sequence,
            'precedenceConstraints': [{'to': k, 'from': v} for k, v in precedence_map.items()],
            'activityFlowGraph': activity_flow_graph,
            'containers': containers,
            'componentSummary': component_summary,
            'packageReferencedTables': pkg_ref_tables['packageReferencedTables'],
            'packageReferencedTablesDetailed': pkg_ref_tables['packageReferencedTablesDetailed'],
        }

        package_id = str(uuid.uuid4())
        _PARSED_PACKAGE_STORE[package_id] = {
            "parsed_data": parsed_data,
            "package_name": metadata.get('packageName', metadata.get('objectName', 'SSISPackage')),
            "original_filename": filename,
            "created_at": time.time(),
        }
        if len(_PARSED_PACKAGE_STORE) > _PARSED_PACKAGE_STORE_MAX:
            oldest = sorted(_PARSED_PACKAGE_STORE.items(), key=lambda kv: kv[1].get("created_at", 0.0))[0][0]
            _PARSED_PACKAGE_STORE.pop(oldest, None)

        return {
            'success': True,
            'message': f'Successfully parsed {len(activities)} activities, {len(variables)} variables',
            'data': parsed_data,
            'formattedXml': formatted_xml,
            'packageId': package_id,
            'originalFilename': filename,
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error parsing file: {str(e)}")


@app.post("/api/generate-control-table-sql")
async def api_generate_control_table_sql(
    request_data: Dict[str, Any] = Body(...)
):
    """
    Generate SQL INSERT scripts for ADF metadata table dbo.ControlTableIntegrated.
    Expects parsed SSIS package data (from /api/parse-dtsx response.data).
    Returns SQL script as text; optional download as file.
    """
    try:
        parsed_data = request_data.get('data', request_data)
        if not isinstance(parsed_data, dict) or 'activities' not in parsed_data:
            raise HTTPException(
                status_code=400,
                detail="Invalid input: Expected parsed SSIS package data (from /api/parse-dtsx). Provide { data: { metadata, activities, ... } }."
            )
        sql_script = generate_control_table_sql(parsed_data)
        return {
            'success': True,
            'sqlScript': sql_script,
            'filename': 'ControlTableIntegrated_Inserts.sql'
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/generate-pyspark-notebooks")
async def api_generate_pyspark_notebooks(
    request_data: Dict[str, Any] = Body(...)
):
    """
    Generate PySpark notebooks for Databricks (Silver/Gold) from parsed Data Flow tasks.
    Expects parsed SSIS package data (from /api/parse-dtsx response.data).
    Returns dict of notebook_name -> notebook_content (downloadable).
    """
    try:
        parsed_data = request_data.get('data', request_data)
        if not isinstance(parsed_data, dict) or 'activities' not in parsed_data:
            raise HTTPException(
                status_code=400,
                detail="Invalid input: Expected parsed SSIS package data (from /api/parse-dtsx)."
            )
        task_name = request_data.get('dataFlowTaskName')
        notebooks = generate_pyspark_notebook(parsed_data, task_name)
        return {
            'success': True,
            'notebooks': notebooks,
            'count': len(notebooks)
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/map-to-fabric")
async def map_to_fabric(
    package_data: Dict[str, Any] = Body(...)
):
    """
    MODULE 6: Map SSIS package to Fabric execution model.
    
    IMPORTANT: This endpoint expects ALREADY-PARSED SSIS package data from /api/parse-dtsx,
    NOT raw XML. It does NOT parse XML files.
    
    Expected input format (from /api/parse-dtsx response.data):
    {
        "metadata": {...},
        "connectionManagers": [...],
        "activities": [...]
    }
    
    Returns mapping trace with Fabric activity mappings and diagnostics.
    """
    if not MAPPING_ENGINE_AVAILABLE or MappingEngine is None:
        raise HTTPException(
            status_code=503,
            detail="Mapping engine not available. Please install dependencies: pip install pyyaml>=6.0.1"
        )
    
    try:
        # Validate that we received parsed data structure, not raw XML or file
        if not isinstance(package_data, dict):
            raise HTTPException(
                status_code=400,
                detail="Invalid input: Expected parsed SSIS package data (dict), not raw XML or file. Please use /api/parse-dtsx first."
            )
        
        # Check for expected parsed data structure
        if 'activities' not in package_data:
            raise HTTPException(
                status_code=400,
                detail="Invalid input: Missing 'activities' field. Expected parsed SSIS package data from /api/parse-dtsx. If you have raw XML, use /api/parse-dtsx first to parse it."
            )
        
        # Use the parsed data directly - NO XML parsing happens here
        mapping_engine = MappingEngine()
        mapping_trace = mapping_engine.generate_mapping_trace(package_data)
        
        return {
            'success': True,
            'mappingTrace': mapping_trace
        }
    except HTTPException:
        raise
    except FileNotFoundError as e:
        raise HTTPException(status_code=500, detail=f"Mapping rules file not found: {str(e)}")
    except Exception as e:
        import traceback
        error_detail = f"Error mapping to Fabric: {str(e)}\n{traceback.format_exc()}"
        raise HTTPException(status_code=500, detail=error_detail)


@app.post("/api/generate-fabric-pipeline")
async def generate_fabric_pipeline(
    request_data: Dict[str, Any] = Body(...)
):
    """
    MODULE 7: Generate Fabric Pipeline JSON from mapped SSIS package.
    
    IMPORTANT: This endpoint expects ALREADY-PARSED SSIS package data from /api/parse-dtsx,
    NOT raw XML. It does NOT parse XML files.
    
    Expected input format:
    {
        "package_data": {
            "metadata": {...},
            "connectionManagers": [...],
            "activities": [...]
        },
        "mapping_trace": {...}  // Optional - will be generated if not provided
    }
    
    If mapping_trace is not provided, it will be generated automatically using the parsed data.
    """
    if MappingEngine is None or FabricPipelineGenerator is None:
        raise HTTPException(
            status_code=503,
            detail="Pipeline generator not available. Please ensure all dependencies are installed."
        )
    
    try:
        # Extract data from request
        package_data = request_data.get('package_data', request_data)
        mapping_trace = request_data.get('mapping_trace')
        
        # Validate that we received parsed data structure, not raw XML or file
        if not isinstance(package_data, dict):
            raise HTTPException(
                status_code=400,
                detail="Invalid input: Expected parsed SSIS package data (dict), not raw XML or file. Please use /api/parse-dtsx first."
            )
        
        # Check for expected parsed data structure
        if 'activities' not in package_data:
            raise HTTPException(
                status_code=400,
                detail="Invalid input: Missing 'activities' field in package_data. Expected parsed SSIS package data from /api/parse-dtsx. If you have raw XML, use /api/parse-dtsx first to parse it."
            )
        
        # Use the parsed data directly - NO XML parsing happens here
        mapping_engine = MappingEngine()
        pipeline_generator = FabricPipelineGenerator(mapping_engine)
        
        # Generate mapping trace if not provided (uses parsed data, not XML)
        if mapping_trace is None:
            mapping_trace = mapping_engine.generate_mapping_trace(package_data)
        
        # Generate pipeline from parsed data and mapping trace
        pipeline = pipeline_generator.generate_pipeline(package_data, mapping_trace)
        
        # Validate pipeline
        validation = pipeline_generator.validate_pipeline(pipeline)
        
        # Generate conversion summary
        summary = pipeline_generator.generate_conversion_summary(mapping_trace, pipeline, validation)
        
        return {
            'success': True,
            'pipeline': pipeline,
            'validation': validation,
            'summary': summary,
            'mappingTrace': mapping_trace
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        error_detail = f"Error generating Fabric pipeline: {str(e)}\n{traceback.format_exc()}"
        raise HTTPException(status_code=500, detail=error_detail)


@app.post("/api/export-fabric-pipeline")
async def export_fabric_pipeline(
    request_data: Dict[str, Any] = Body(...)
):
    """
    MODULE 8: Export Fabric Pipeline JSON for download.
    Returns JSON response that can be downloaded by frontend.
    """
    try:
        # Extract data from request
        pipeline = request_data.get('pipeline', request_data)
        filename = request_data.get('filename')
        
        # Generate filename if not provided
        if not filename:
            pipeline_name = pipeline.get('name', 'fabric_pipeline')
            filename = f"{pipeline_name}.json"
        
        # Convert to JSON string
        json_content = json.dumps(pipeline, indent=2, ensure_ascii=False)
        
        # Return as downloadable JSON
        return Response(
            content=json_content,
            media_type="application/json",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"'
            }
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error exporting pipeline: {str(e)}")


@app.post("/api/validate-fabric-pipeline")
async def validate_fabric_pipeline(
    pipeline: Dict[str, Any] = Body(...)
):
    """
    MODULE 7: Validate Fabric Pipeline JSON.
    Returns validation results with errors and warnings.
    """
    if FabricPipelineGenerator is None:
        raise HTTPException(
            status_code=503,
            detail="Pipeline generator not available. Please ensure all dependencies are installed."
        )
    
    try:
        pipeline_generator = FabricPipelineGenerator()
        validation = pipeline_generator.validate_pipeline(pipeline)
        
        return {
            'success': True,
            'validation': validation
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error validating pipeline: {str(e)}")


@app.post("/api/classify-activity")
async def classify_activity(
    activity: Dict[str, Any] = Body(...)
):
    """
    MODULE 6: Classify a single SSIS activity.
    Returns classification, confidence score, and diagnostics.
    """
    if not MAPPING_ENGINE_AVAILABLE or MappingEngine is None:
        raise HTTPException(
            status_code=503,
            detail="Mapping engine not available. Please ensure all dependencies are installed."
        )
    
    try:
        mapping_engine = MappingEngine()
        classification = mapping_engine.classify_component(activity)
        
        return {
            'success': True,
            'classification': classification
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error classifying activity: {str(e)}")


@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    import inspect
    registered_routes = [route.path for route in app.routes if hasattr(route, 'path')]
    return {
        "status": "healthy",
        "modules": {
            "mappingEngine": MAPPING_ENGINE_AVAILABLE,
            "pipelineGenerator": FabricPipelineGenerator is not None
        },
        "endpoints": {
            "mapToFabric": "/api/map-to-fabric" in registered_routes,
            "generatePipeline": "/api/generate-fabric-pipeline" in registered_routes,
            "exportPipeline": "/api/export-fabric-pipeline" in registered_routes,
            "validatePipeline": "/api/validate-fabric-pipeline" in registered_routes,
            "classifyActivity": "/api/classify-activity" in registered_routes
        },
        "allRoutes": registered_routes
    }


@app.post("/api/migration-package-commands")
async def get_migration_package_commands(request_data: Dict[str, Any] = Body(...)):
    """
    Return CLI commands for all pipeline/job JSONs that would be generated from the migration package.
    Expects parsed SSIS package data (from /api/parse-dtsx response.data).
    """
    if not MIGRATION_ARTIFACTS_AVAILABLE or MigrationArtifactGenerator is None:
        raise HTTPException(status_code=500, detail="MigrationArtifactGenerator is not available on the server.")
    parsed_data = request_data.get("data", request_data)
    if not isinstance(parsed_data, dict) or "activities" not in parsed_data:
        raise HTTPException(
            status_code=400,
            detail="Invalid input: Expected parsed SSIS package data (from /api/parse-dtsx). Provide { data: { metadata, activities, ... } }.",
        )
    gen = MigrationArtifactGenerator()
    commands = gen.get_cli_commands(parsed_data)
    return {"success": True, "commands": commands}


@app.get("/api/migration-package/{packageId}")
async def get_migration_package(packageId: str, background_tasks: BackgroundTasks):
    """
    Generate a downloadable ZIP migration bundle using already-parsed SSIS metadata stored for the packageId.
    """
    if not MIGRATION_ARTIFACTS_AVAILABLE or MigrationArtifactGenerator is None:
        raise HTTPException(status_code=500, detail="MigrationArtifactGenerator is not available on the server.")

    entry = _PARSED_PACKAGE_STORE.get(packageId)
    if not entry or not isinstance(entry, dict) or "parsed_data" not in entry:
        raise HTTPException(status_code=404, detail="Unknown packageId or metadata has expired. Parse the package again.")

    parsed_data = entry["parsed_data"]
    # Use parsed file name (without extension) for folder and zip name
    original_filename = entry.get("original_filename") or ""
    for ext in (".xml", ".dtsx"):
        if original_filename.lower().endswith(ext):
            original_filename = original_filename[: -len(ext)]
            break
    base_name = re.sub(r"[^A-Za-z0-9._-]+", "_", str(original_filename).strip()).strip("_") or "SSISPackage"

    temp_root = Path(tempfile.mkdtemp(prefix="migration_pkg_"))
    zip_path = temp_root / f"{base_name}_migration_package.zip"

    try:
        gen = MigrationArtifactGenerator()
        gen.build_migration_package(parsed_data, temp_root, source_filename=base_name)

        pkg_dir = temp_root / f"{base_name}_migration_package"
        with zipfile.ZipFile(str(zip_path), "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for p in pkg_dir.rglob("*"):
                if p.is_dir():
                    continue
                zf.write(str(p), arcname=str(p.relative_to(temp_root)))
    except Exception as e:
        shutil.rmtree(temp_root, ignore_errors=True)
        raise HTTPException(status_code=500, detail=f"Failed to build migration package: {str(e)}")

    background_tasks.add_task(shutil.rmtree, temp_root, True)

    return FileResponse(
        path=str(zip_path),
        media_type="application/zip",
        filename=f"{base_name}_migration_package.zip",
    )


if __name__ == "__main__":
    import uvicorn
    # Run with increased limits for large SSIS package data
    # Note: Starlette/FastAPI don't have a hard body size limit by default,
    # but uvicorn has some limits. For very large payloads, consider using
    # a reverse proxy like nginx with increased client_max_body_size
    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=8000,
        # Increase timeout for large requests
        timeout_keep_alive=300,
    )
