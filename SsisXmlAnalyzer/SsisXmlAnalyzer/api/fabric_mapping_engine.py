"""
MODULE 6: SSIS → Fabric Mapping Engine (Backend)
Translates SSIS semantic model into Fabric-compatible execution model.
"""

import yaml
import json
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path
from enum import Enum


class SupportLevel(Enum):
    FULL = "full"
    PARTIAL = "partial"
    UNSUPPORTED = "unsupported"


class ConfidenceLevel(Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    CONDITIONAL = "conditional"


class MappingEngine:
    """Rule-based transformation engine for SSIS → Fabric mapping."""
    
    def __init__(self, rules_file: Optional[str] = None):
        """Initialize mapping engine with rules configuration."""
        if rules_file is None:
            rules_file = Path(__file__).parent / "mapping_rules.yaml"
        
        self.rules_file = Path(rules_file)
        self.rules = self._load_rules()
        self.mapping_cache = {}
        
    def _load_rules(self) -> Dict[str, Any]:
        """Load mapping rules from YAML file."""
        try:
            with open(self.rules_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"Mapping rules file not found: {self.rules_file}")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing mapping rules YAML: {e}")
    
    def map_activity(self, activity: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map a single SSIS activity to Fabric activity.
        
        Args:
            activity: SSIS activity dictionary
            
        Returns:
            Dictionary with mapping result including:
            - target: Fabric activity type
            - confidence: Confidence score (0-1)
            - supportLevel: Support level (full/partial/unsupported)
            - warnings: List of warnings
            - semanticMismatches: List of semantic feature mismatches
            - mapping: Mapped properties
        """
        activity_type = self._normalize_activity_type(activity.get('type', ''))
        
        # Use specialized mapping for Execute SQL Task to detect semantic mismatches
        if activity_type == 'Execute SQL Task':
            specialized_result = self.map_execute_sql_task(activity)
            # Merge with base result structure
            result = {
                'ssisActivityId': activity.get('id'),
                'ssisActivityName': activity.get('name'),
                'ssisActivityType': activity_type,
                'targetActivityType': specialized_result['targetActivityType'],
                'confidence': specialized_result['confidence'],
                'supportLevel': specialized_result['supportLevel'],
                'warnings': specialized_result['warnings'],
                'semanticMismatches': specialized_result.get('semanticMismatches', []),
                'unsupportedFeatures': specialized_result.get('unsupportedFeatures', []),
                'requiresManualIntervention': specialized_result['requiresManualIntervention'],
                'mapping': specialized_result['mapping'],
                'summary': specialized_result.get('summary', {})
            }
            return result
        
        # Check if we have a mapping rule for this activity type
        mapping_rule = self.rules.get('mappings', {}).get(activity_type)
        
        if not mapping_rule:
            # Try to find a generic mapping
            return self._create_unsupported_mapping(activity, "No mapping rule found")
        
        # Apply mapping rule
        result = {
            'ssisActivityId': activity.get('id'),
            'ssisActivityName': activity.get('name'),
            'ssisActivityType': activity_type,
            'targetActivityType': mapping_rule.get('target'),
            'confidence': self._get_confidence_score(mapping_rule.get('confidence', 'low')),
            'supportLevel': mapping_rule.get('supportLevel', 'partial'),
            'warnings': mapping_rule.get('warnings', []),
            'unsupported': mapping_rule.get('unsupported', []),
            'semanticMismatches': [],  # Initialize for consistency
            'notes': mapping_rule.get('notes', ''),
            'mapping': {},
            'requiresManualIntervention': mapping_rule.get('supportLevel') != 'full'
        }
        
        # Apply conditional mapping if available
        if 'conditions' in mapping_rule:
            conditional_result = self._evaluate_conditions(activity, mapping_rule['conditions'])
            if conditional_result:
                result.update(conditional_result)
        
        # Map properties
        if 'mapping' in mapping_rule:
            result['mapping'] = self._map_properties(activity, mapping_rule['mapping'])
        
        # Check for unsupported features
        if 'unsupported' in mapping_rule:
            detected_unsupported = self._detect_unsupported_features(activity, mapping_rule['unsupported'])
            if detected_unsupported:
                result['detectedUnsupportedFeatures'] = detected_unsupported
                result['requiresManualIntervention'] = True
        
        return result
    
    def map_execute_sql_task(self, activity: Dict[str, Any]) -> Dict[str, Any]:
        """
        Specialized mapping for Execute SQL Task with semantic mismatch detection.
        
        SSIS Execute SQL Task supports features that Fabric SqlScript does NOT:
        - Result sets (can return data to variables)
        - Output parameters (OUTPUT/OUT parameters)
        - Variable mutation (setting SSIS variables from SQL results)
        - Transaction scope (participation in package transactions)
        """
        sql_props = activity.get('sqlTaskProperties', {})
        
        # Start with baseline mapping
        result = {
            'targetActivityType': 'SQLScript',
            'confidence': 1.0,  # Will be reduced based on unsupported features
            'supportLevel': 'full',
            'warnings': [],
            'semanticMismatches': [],
            'unsupportedFeatures': [],
            'requiresManualIntervention': False,
            'mapping': {
                'sqlStatement': sql_props.get('sqlStatementSource', activity.get('sqlCommand', '')),
                'connectionManager': sql_props.get('connection') or activity.get('connectionId'),
                'timeout': sql_props.get('timeout', '0'),
                'parameters': []
            }
        }
        
        # Check for Result Sets (SEMANTIC MISMATCH)
        result_set_type = sql_props.get('resultSetType', 'None')
        result_bindings = sql_props.get('resultBindings', [])
        
        if result_set_type and result_set_type != 'None' and result_set_type != '0':
            result['semanticMismatches'].append({
                'feature': 'Result Sets',
                'ssisSupport': f"SSIS supports result sets (type: {result_set_type})",
                'fabricSupport': 'Fabric SqlScript does NOT support returning result sets',
                'impact': 'HIGH',
                'remediation': 'Use separate Lookup activity or Mapping Data Flow to retrieve results, or execute in Notebook',
                'variableMutations': [binding.get('variableName') for binding in result_bindings if binding.get('variableName')]
            })
            result['confidence'] = min(result['confidence'], 0.4)
            result['supportLevel'] = 'partial'
            result['requiresManualIntervention'] = True
            result['warnings'].append(f"Result sets (type: {result_set_type}) not supported - will lose variable assignment capability")
        
        # Check for Output Parameters (SEMANTIC MISMATCH)
        parameter_bindings = sql_props.get('parameterBindings', [])
        output_parameters = [
            param for param in parameter_bindings 
            if param.get('direction', '').upper() in ['OUTPUT', 'OUT', 'RETURN']
        ]
        
        if output_parameters:
            output_param_names = [p.get('name') for p in output_parameters]
            result['semanticMismatches'].append({
                'feature': 'Output Parameters',
                'ssisSupport': f"SSIS supports OUTPUT parameters: {', '.join(output_param_names)}",
                'fabricSupport': 'Fabric SqlScript does NOT support OUTPUT parameters',
                'impact': 'HIGH',
                'remediation': 'Refactor to use result sets via SELECT statements, or use Notebook activity',
                'affectedParameters': output_param_names,
                'variableMutations': [p.get('variableName') for p in output_parameters if p.get('variableName')]
            })
            result['confidence'] = min(result['confidence'], 0.5)
            result['supportLevel'] = 'partial'
            result['requiresManualIntervention'] = True
            result['warnings'].append(f"Output parameters not supported - {len(output_parameters)} parameter(s) will lose variable assignment")
        
        # Map input parameters (these ARE supported)
        input_parameters = [
            param for param in parameter_bindings 
            if param.get('direction', '').upper() in ['INPUT', 'IN', ''] or not param.get('direction')
        ]
        
        if input_parameters:
            result['mapping']['parameters'] = [
                {
                    'name': param.get('name'),
                    'variableName': param.get('variableName'),
                    'direction': param.get('direction', 'Input'),
                    'dataType': param.get('dataType'),
                    'supported': True
                }
                for param in input_parameters
            ]
        
        # Add output parameters to mapping but mark as unsupported
        if output_parameters:
            result['mapping']['unsupportedParameters'] = [
                {
                    'name': param.get('name'),
                    'variableName': param.get('variableName'),
                    'direction': param.get('direction'),
                    'dataType': param.get('dataType'),
                    'supported': False,
                    'reason': 'Fabric SqlScript does not support OUTPUT parameters'
                }
                for param in output_parameters
            ]
        
        # Check for Variable Mutations (from result bindings or output params)
        all_variable_mutations = []
        if result_bindings:
            all_variable_mutations.extend([b.get('variableName') for b in result_bindings if b.get('variableName')])
        if output_parameters:
            all_variable_mutations.extend([p.get('variableName') for p in output_parameters if p.get('variableName')])
        
        if all_variable_mutations:
            unique_mutations = list(set(all_variable_mutations))
            result['semanticMismatches'].append({
                'feature': 'Variable Mutation',
                'ssisSupport': f"SSIS can set variables from SQL results: {', '.join(unique_mutations)}",
                'fabricSupport': 'Fabric SqlScript cannot mutate pipeline parameters',
                'impact': 'HIGH',
                'remediation': 'Use Set Variable activity or refactor logic to avoid variable mutations',
                'affectedVariables': unique_mutations
            })
            result['warnings'].append(f"Variable mutations not supported - {len(unique_mutations)} variable(s) will not be updated")
        
        # Check for Transaction Scope (SEMANTIC MISMATCH)
        # SSIS can participate in package-level transactions, Fabric SqlScript cannot
        # This is harder to detect from properties, but we can check for transaction-related properties
        properties = activity.get('properties', [])
        transaction_related = [
            p for p in properties 
            if 'transaction' in p.get('name', '').lower() or 'isolation' in p.get('name', '').lower()
        ]
        
        if transaction_related or activity.get('transactionOption'):
            result['semanticMismatches'].append({
                'feature': 'Transaction Scope',
                'ssisSupport': 'SSIS Execute SQL Task can participate in package transactions',
                'fabricSupport': 'Fabric SqlScript cannot participate in transaction scope',
                'impact': 'MEDIUM',
                'remediation': 'Refactor transaction logic at pipeline level or use Execute Pipeline activity',
                'note': 'Each SqlScript activity runs in its own transaction context'
            })
            result['confidence'] = min(result['confidence'], 0.7)
            if result['supportLevel'] == 'full':
                result['supportLevel'] = 'partial'
            result['warnings'].append("Transaction scope not supported - each SqlScript runs independently")
        
        # Check for stored procedure with return values
        if sql_props.get('isStoredProcedure'):
            # If it's a stored procedure AND has output params/result sets, it's problematic
            if output_parameters or result_bindings:
                result['warnings'].append("Stored procedure with return values not fully supported")
                result['requiresManualIntervention'] = True
            else:
                result['warnings'].append("Stored procedure execution supported but output handling may differ")
        
        # Detect dynamic SQL expressions
        sql_statement = result['mapping']['sqlStatement']
        if sql_statement and ('@' in sql_statement or '@[' in sql_statement):
            result['warnings'].append("Dynamic SQL expressions need manual validation")
            result['confidence'] = min(result['confidence'], 0.8)
        
        # Final confidence adjustment based on semantic mismatches
        if result['semanticMismatches']:
            # Reduce confidence based on number of mismatches
            mismatch_penalty = 0.3 * len(result['semanticMismatches'])
            result['confidence'] = max(0.2, result['confidence'] - mismatch_penalty)
        
        # Add summary
        result['summary'] = {
            'canAutoConvert': len(result['semanticMismatches']) == 0,
            'totalMismatches': len(result['semanticMismatches']),
            'affectedVariables': len(all_variable_mutations) if all_variable_mutations else 0,
            'requiresRefactoring': result['requiresManualIntervention']
        }
        
        return result
    
    def map_data_flow_task(self, activity: Dict[str, Any]) -> Dict[str, Any]:
        """Specialized mapping for Data Flow Task."""
        components = activity.get('components', [])
        
        # Classify data flow complexity
        sources = [c for c in components if c.get('componentType') == 'Source']
        destinations = [c for c in components if c.get('componentType') == 'Destination']
        transformations = [c for c in components if c.get('componentType') == 'Transformation']
        
        # Simple case: single source to single destination
        if len(sources) == 1 and len(destinations) == 1 and len(transformations) == 0:
            return {
                'targetActivityType': 'Copy',
                'confidence': 0.9,
                'supportLevel': 'full',
                'warnings': [],
                'mapping': {
                    'source': self._map_data_flow_component(sources[0]),
                    'destination': self._map_data_flow_component(destinations[0])
                }
            }
        
        # Complex case: multiple components or transformations
        return {
            'targetActivityType': 'MappingDataFlow',
            'confidence': 0.6,
            'supportLevel': 'partial',
            'fallback': 'Notebook',
            'warnings': [
                "Complex transformations require Mapping Data Flow or Notebook",
                "Error outputs need manual mapping" if any(c.get('errorOutput') for c in components) else None
            ],
            'warnings': [w for w in [
                "Complex transformations require Mapping Data Flow or Notebook",
                "Error outputs need manual mapping" if any(c.get('errorOutput') for c in components) else None,
                "Multiple outputs require manual intervention" if len(destinations) > 1 else None
            ] if w],
            'mapping': {
                'sources': [self._map_data_flow_component(s) for s in sources],
                'destinations': [self._map_data_flow_component(d) for d in destinations],
                'transformations': [self._map_data_flow_component(t) for t in transformations]
            },
            'requiresManualIntervention': True
        }
    
    def map_lookup_transform(self, component: Dict[str, Any]) -> Dict[str, Any]:
        """Specialized mapping for Lookup Transform."""
        properties = component.get('properties', [])
        
        # Extract cache mode
        cache_mode = next((p.get('value') for p in properties if p.get('name') == 'CacheMode'), 'Full')
        
        result = {
            'targetActivityType': 'MappingDataFlow',
            'confidence': 0.7 if cache_mode != 'NoCache' else 0.4,
            'supportLevel': 'partial',
            'fallback': 'Notebook',
            'warnings': []
        }
        
        if cache_mode == 'NoCache':
            result['warnings'].append("Performance risk for large datasets in no-cache mode")
            result['requiresManualIntervention'] = True
        
        result['warnings'].append("Manual tuning required for optimal performance")
        
        return result
    
    def map_flat_file_source(self, component: Dict[str, Any]) -> Dict[str, Any]:
        """Specialized mapping for Flat File Source."""
        properties = {p.get('name'): p.get('value') for p in component.get('properties', [])}
        conn_details = component.get('connectionDetails', {})
        
        return {
            'targetActivityType': 'ADLSGen2',
            'confidence': 0.9,
            'supportLevel': 'full',
            'mapping': {
                'filePath': conn_details.get('connectionString') or properties.get('FilePath'),
                'delimiter': properties.get('ColumnDelimiter', ','),
                'textQualifier': properties.get('TextQualifier', '"'),
                'encoding': properties.get('CodePage', 'UTF-8'),
                'firstRowHeader': properties.get('FirstRowHeader', 'false').lower() == 'true'
            },
            'schemaInference': True,
            'warnings': []
        }
    
    def map_ole_db_source(self, component: Dict[str, Any]) -> Dict[str, Any]:
        """Specialized mapping for OLE DB Source."""
        properties = {p.get('name'): p.get('value') for p in component.get('properties', [])}
        conn_details = component.get('connectionDetails', {})
        # Prefer direct SqlCommand; fall back to sourceMetadata.sourceQuery (resolved from SqlCommandVariable)
        source_meta = component.get('sourceMetadata') or {}
        query = properties.get('SqlCommand') or source_meta.get('sourceQuery') or ''
        
        # Determine if Fabric Warehouse or SQL Endpoint
        target = 'FabricWarehouse' if conn_details.get('initialCatalog') else 'SQLEndpoint'
        
        return {
            'targetActivityType': target,
            'confidence': 0.9,
            'supportLevel': 'full',
            'mapping': {
                'query': query,
                'tableName': properties.get('TableName'),
                'connectionManager': component.get('connectionId'),
                'dataSource': conn_details.get('dataSource'),
                'database': conn_details.get('initialCatalog')
            },
            'warnings': [w for w in [
                "SSIS-specific query hints may not be supported" if 'WITH' in (query or '').upper() else None
            ] if w]
        }
    
    def map_foreach_loop(self, activity: Dict[str, Any]) -> Dict[str, Any]:
        """Specialized mapping for ForEach Loop Container."""
        properties = {p.get('name'): p.get('value') for p in activity.get('properties', [])}
        
        return {
            'targetActivityType': 'ForEach',
            'confidence': 0.8,
            'supportLevel': 'partial',
            'mapping': {
                'enumeratorType': properties.get('EnumeratorType', 'File'),
                'variableMappings': properties.get('VariableMappings', [])
            },
            'supportedEnumerators': ['File', 'ADO_Recordset', 'Variable'],
            'warnings': [
                "Nested loops may require manual refactoring",
                "Complex variable expressions need validation"
            ],
            'requiresManualIntervention': True
        }
    
    def map_variables(self, package_data: Dict[str, Any]) -> Dict[str, Any]:
        """Map SSIS variables to Fabric pipeline parameters."""
        variables = package_data.get('variables', [])
        
        pipeline_parameters = []
        for var in variables:
            param = {
                'name': var.get('name'),
                'type': self._map_variable_type(var.get('dataType', 'System.String')),
                'defaultValue': var.get('value'),
                'ssisVariableName': var.get('name')
            }
            pipeline_parameters.append(param)
        
        return {
            'targetActivityType': 'PipelineParameters',
            'confidence': 0.9,
            'supportLevel': 'full',
            'mapping': {
                'parameters': pipeline_parameters
            },
            'warnings': []
        }
    
    def classify_component(self, activity: Dict[str, Any]) -> Dict[str, Any]:
        """
        Classify component with support level and confidence score.
        
        Returns:
            Dictionary with classification, confidence, and diagnostics
        """
        mapping_result = self.map_activity(activity)
        
        return {
            'componentId': activity.get('id'),
            'componentName': activity.get('name'),
            'componentType': activity.get('type'),
            'classification': self._get_support_level_display(mapping_result.get('supportLevel', 'partial')),
            'confidenceScore': mapping_result.get('confidence', 0.5),
            'targetType': mapping_result.get('targetActivityType'),
            'warnings': mapping_result.get('warnings', []),
            'semanticMismatches': mapping_result.get('semanticMismatches', []),
            'requiresManualIntervention': mapping_result.get('requiresManualIntervention', False),
            'explanation': self._generate_explanation(mapping_result)
        }
    
    def generate_mapping_trace(self, package_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate complete mapping trace for entire package.
        
        Returns:
            Dictionary with:
            - mappedActivities: List of mapped activities
            - diagnostics: Conversion diagnostics
            - confidenceScore: Overall confidence score
            - manualRemediationList: List of items requiring manual intervention
        """
        activities = package_data.get('activities', [])
        mapped_activities = []
        all_warnings = []
        all_semantic_mismatches = []
        manual_remediation = []
        confidence_scores = []
        
        for activity in activities:
            mapping_result = self.map_activity(activity)
            classification = self.classify_component(activity)
            
            mapped_activities.append({
                'ssis': {
                    'id': activity.get('id'),
                    'name': activity.get('name'),
                    'type': activity.get('type')
                },
                'fabric': {
                    'activityType': mapping_result.get('targetActivityType'),
                    'mapping': mapping_result.get('mapping', {})
                },
                'classification': classification,
                'mappingResult': mapping_result
            })
            
            all_warnings.extend(mapping_result.get('warnings', []))
            confidence_scores.append(mapping_result.get('confidence', 0.5))
            
            # Collect semantic mismatches
            semantic_mismatches = mapping_result.get('semanticMismatches', [])
            if semantic_mismatches:
                all_semantic_mismatches.extend(semantic_mismatches)
                for mismatch in semantic_mismatches:
                    all_warnings.append(f"Semantic mismatch: {mismatch.get('feature')} - {mismatch.get('impact')} impact")
            
            if mapping_result.get('requiresManualIntervention'):
                remediation_entry = {
                    'activityId': activity.get('id'),
                    'activityName': activity.get('name'),
                    'activityType': activity.get('type'),
                    'reason': mapping_result.get('warnings', []),
                    'semanticMismatches': semantic_mismatches,
                    'targetType': mapping_result.get('targetActivityType')
                }
                
                # Add specific remediation guidance for semantic mismatches
                if semantic_mismatches:
                    remediation_entry['remediationGuidance'] = [
                        mismatch.get('remediation') for mismatch in semantic_mismatches
                    ]
                
                manual_remediation.append(remediation_entry)
        
        # Map variables
        variables_mapping = self.map_variables(package_data)
        
        # Calculate overall confidence
        overall_confidence = sum(confidence_scores) / len(confidence_scores) if confidence_scores else 0.5
        
        # Calculate semantic mismatch statistics
        semantic_mismatch_summary = {}
        for mismatch in all_semantic_mismatches:
            feature = mismatch.get('feature', 'Unknown')
            if feature not in semantic_mismatch_summary:
                semantic_mismatch_summary[feature] = {
                    'count': 0,
                    'impact': mismatch.get('impact', 'UNKNOWN'),
                    'affectedActivities': []
                }
            semantic_mismatch_summary[feature]['count'] += 1
        
        return {
            'mappedActivities': mapped_activities,
            'variablesMapping': variables_mapping,
            'diagnostics': {
                'totalActivities': len(activities),
                'mappedActivities': len(mapped_activities),
                'overallConfidence': overall_confidence,
                'warnings': all_warnings,
                'manualRemediationCount': len(manual_remediation),
                'semanticMismatches': {
                    'total': len(all_semantic_mismatches),
                    'uniqueFeatures': len(semantic_mismatch_summary),
                    'breakdown': semantic_mismatch_summary,
                    'details': all_semantic_mismatches
                }
            },
            'manualRemediationList': manual_remediation,
            'conversionSummary': {
                'fullySupported': len([m for m in mapped_activities if m['classification']['classification'] == '✅ Fully supported']),
                'partiallySupported': len([m for m in mapped_activities if m['classification']['classification'] == '⚠ Partially supported']),
                'unsupported': len([m for m in mapped_activities if m['classification']['classification'] == '❌ Unsupported']),
                'semanticMismatchCount': len(all_semantic_mismatches)
            }
        }
    
    # Helper methods
    
    def _normalize_activity_type(self, activity_type: str) -> str:
        """Normalize SSIS activity type name to match rules."""
        # Remove common prefixes and normalize
        normalized = activity_type.replace(' ', '').replace('Task', '').replace('Container', '')
        
        # Common mappings
        type_mapping = {
            'ExecuteSQL': 'ExecuteSQLTask',
            'DataFlow': 'DataFlowTask',
            'Pipeline': 'DataFlowTask',
            'ForEachLoop': 'ForEachLoopContainer',
            'ForLoop': 'ForLoopContainer',
            'Sequence': 'SequenceContainer'
        }
        
        for key, value in type_mapping.items():
            if key in normalized:
                return value
        
        return normalized or activity_type
    
    def _get_confidence_score(self, confidence_level: str) -> float:
        """Convert confidence level string to numeric score."""
        scoring = self.rules.get('confidenceScoring', {})
        return scoring.get(confidence_level, 0.5)
    
    def _get_support_level_display(self, support_level: str) -> str:
        """Get display string for support level."""
        mapping = self.rules.get('supportLevelMapping', {})
        return mapping.get(support_level, '⚠ Partially supported')
    
    def _map_properties(self, activity: Dict[str, Any], property_mapping: Dict[str, str]) -> Dict[str, Any]:
        """Map SSIS properties to Fabric properties using mapping rules."""
        result = {}
        for fabric_prop, ssis_path in property_mapping.items():
            # Simple path resolution (can be enhanced)
            value = self._resolve_path(activity, ssis_path)
            if value is not None:
                result[fabric_prop] = value
        return result
    
    def _resolve_path(self, obj: Dict[str, Any], path: str) -> Any:
        """Resolve a dot-notation path in an object."""
        parts = path.split('.')
        current = obj
        for part in parts:
            if isinstance(current, dict):
                current = current.get(part)
            elif isinstance(current, list) and part.isdigit():
                current = current[int(part)] if int(part) < len(current) else None
            else:
                return None
            if current is None:
                return None
        return current
    
    def _evaluate_conditions(self, activity: Dict[str, Any], conditions: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Evaluate conditional mapping rules."""
        # Simple condition evaluation (can be enhanced with full expression parser)
        for condition_name, condition_def in conditions.items():
            check = condition_def.get('check', '')
            # Simple evaluation (implement full expression parser if needed)
            if self._evaluate_check(activity, check):
                return {
                    'targetActivityType': condition_def.get('target'),
                    'confidence': self._get_confidence_score(condition_def.get('confidence', 'medium')),
                    'fallback': condition_def.get('fallback')
                }
        return None
    
    def _evaluate_check(self, activity: Dict[str, Any], check: str) -> bool:
        """Simple check evaluator (can be enhanced)."""
        # Very basic implementation - enhance for production
        try:
            # Safe evaluation context
            components = activity.get('components', [])
            has_transformations = any(c.get('componentType') == 'Transformation' for c in components)
            
            # Simple pattern matching
            if 'components.length == 2' in check and len(components) == 2:
                return True
            if 'hasTransformations' in check and has_transformations:
                return True
        except:
            pass
        return False
    
    def _detect_unsupported_features(self, activity: Dict[str, Any], unsupported_list: List[str]) -> List[str]:
        """Detect if activity uses unsupported features."""
        detected = []
        for feature in unsupported_list:
            if self._check_feature(activity, feature):
                detected.append(feature)
        return detected
    
    def _check_feature(self, activity: Dict[str, Any], feature: str) -> bool:
        """Check if activity has a specific feature."""
        # Implement feature detection logic
        if feature == 'resultSetBindings':
            sql_props = activity.get('sqlTaskProperties', {})
            return bool(sql_props.get('resultBindings'))
        elif feature == 'errorOutputs':
            components = activity.get('components', [])
            return any(c.get('errorOutput') for c in components)
        # Add more feature checks as needed
        return False
    
    def _map_data_flow_component(self, component: Dict[str, Any]) -> Dict[str, Any]:
        """Map a data flow component to Fabric source/destination."""
        comp_type = component.get('componentType', '')
        
        if comp_type == 'Source':
            # Determine source type
            conn_details = component.get('connectionDetails', {})
            creation_name = conn_details.get('creationName', '')
            
            if 'FlatFile' in creation_name:
                return self.map_flat_file_source(component)
            elif 'OLEDB' in creation_name or 'OLE DB' in creation_name:
                return self.map_ole_db_source(component)
            else:
                return {'targetActivityType': 'ADLSGen2', 'mapping': {}}
        
        elif comp_type == 'Destination':
            conn_details = component.get('connectionDetails', {})
            return {
                'targetActivityType': 'FabricWarehouse',
                'tableName': component.get('tableName'),
                'connectionDetails': conn_details
            }
        
        return {}
    
    def _map_variable_type(self, ssis_type: str) -> str:
        """Map SSIS variable type to Fabric parameter type."""
        type_mapping = {
            'System.String': 'String',
            'System.Int32': 'Int',
            'System.Int64': 'Int',
            'System.Boolean': 'Bool',
            'System.DateTime': 'String',  # Fabric uses String for DateTime
            'System.Double': 'Float',
            'System.Single': 'Float'
        }
        return type_mapping.get(ssis_type, 'String')
    
    def _create_unsupported_mapping(self, activity: Dict[str, Any], reason: str) -> Dict[str, Any]:
        """Create mapping result for unsupported activity."""
        return {
            'ssisActivityId': activity.get('id'),
            'ssisActivityName': activity.get('name'),
            'ssisActivityType': activity.get('type'),
            'targetActivityType': None,
            'confidence': 0.0,
            'supportLevel': 'unsupported',
            'warnings': [reason],
            'requiresManualIntervention': True,
            'mapping': {}
        }
    
    def _generate_explanation(self, mapping_result: Dict[str, Any]) -> str:
        """Generate human-readable explanation for mapping."""
        support_level = mapping_result.get('supportLevel', 'partial')
        target_type = mapping_result.get('targetActivityType', 'Unknown')
        notes = mapping_result.get('notes', '')
        
        if support_level == 'full':
            return f"Fully supported mapping to {target_type}. {notes}"
        elif support_level == 'partial':
            warnings = mapping_result.get('warnings', [])
            warning_text = ' '.join(warnings[:2]) if warnings else ''
            return f"Partially supported mapping to {target_type}. {warning_text}"
        else:
            return f"Unsupported activity type. Manual conversion required."

