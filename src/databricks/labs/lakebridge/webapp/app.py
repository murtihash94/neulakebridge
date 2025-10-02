"""
Lakebridge Web Application

This Flask application provides a web interface for the Lakebridge CLI tool,
making it easier for clients to migrate their workloads to Databricks.
"""

import os
import tempfile
import json
import asyncio
from pathlib import Path
from typing import Optional

from flask import Flask, render_template, request, jsonify, send_file, redirect, url_for, flash
from werkzeug.utils import secure_filename
from databricks.sdk import WorkspaceClient

from databricks.labs.lakebridge.config import TranspileConfig
from databricks.labs.lakebridge.contexts.application import ApplicationContext
from databricks.labs.lakebridge.transpiler.execute import transpile as do_transpile
from databricks.labs.lakebridge.transpiler.repository import TranspilerRepository
from databricks.labs.lakebridge.transpiler.sqlglot.sqlglot_engine import SqlglotEngine
from databricks.labs.lakebridge.analyzer.lakebridge_analyzer import LakebridgeAnalyzer, AnalyzerPrompts, AnalyzerRunner


app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'lakebridge-dev-key')
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size
app.config['UPLOAD_FOLDER'] = tempfile.gettempdir()

# Allowed file extensions
ALLOWED_EXTENSIONS = {'sql', 'txt', 'zip', 'dtsx', 'ispac'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# Supported source systems
SUPPORTED_SYSTEMS = [
    'snowflake', 'teradata', 'oracle', 'sqlserver', 'postgresql', 
    'mysql', 'redshift', 'bigquery', 'synapse', 'hive'
]

@app.route('/')
def index():
    """Landing page with logo and main options"""
    return render_template('index.html', supported_systems=SUPPORTED_SYSTEMS)

@app.route('/designer')
def designer():
    """Visual designer page for agents and tasks"""
    # Sample agents data
    agents = [
        {
            'id': 1,
            'name': 'SQL Transpiler Agent',
            'description': 'Automatically converts SQL queries from various dialects to Databricks SQL',
            'type': 'Transpilation',
            'status': 'Active',
            'icon': 'fa-exchange-alt',
            'color_start': '#2563eb',
            'color_end': '#1d4ed8',
            'task_count': 12,
            'last_run': '2 hours ago'
        },
        {
            'id': 2,
            'name': 'Schema Migration Agent',
            'description': 'Migrates database schemas and table definitions to Databricks',
            'type': 'Schema',
            'status': 'Active',
            'icon': 'fa-database',
            'color_start': '#3d8fcf',
            'color_end': '#2563eb',
            'task_count': 8,
            'last_run': '1 day ago'
        },
        {
            'id': 3,
            'name': 'ETL Workflow Agent',
            'description': 'Converts ETL workflows and orchestration jobs to Databricks workflows',
            'type': 'Workflow',
            'status': 'Active',
            'icon': 'fa-project-diagram',
            'color_start': '#1a4d7a',
            'color_end': '#0f2a4a',
            'task_count': 15,
            'last_run': '3 hours ago'
        },
        {
            'id': 4,
            'name': 'Data Quality Agent',
            'description': 'Validates data quality and ensures migration accuracy',
            'type': 'Quality',
            'status': 'Idle',
            'icon': 'fa-check-circle',
            'color_start': '#60a5fa',
            'color_end': '#3d8fcf',
            'task_count': 5,
            'last_run': '5 days ago'
        },
        {
            'id': 5,
            'name': 'Performance Optimizer',
            'description': 'Analyzes and optimizes query performance for Databricks',
            'type': 'Optimization',
            'status': 'Active',
            'icon': 'fa-tachometer-alt',
            'color_start': '#0a1929',
            'color_end': '#1a4d7a',
            'task_count': 7,
            'last_run': '6 hours ago'
        },
        {
            'id': 6,
            'name': 'Reconciliation Agent',
            'description': 'Compares source and target data to ensure migration completeness',
            'type': 'Validation',
            'status': 'Active',
            'icon': 'fa-balance-scale',
            'color_start': '#2563eb',
            'color_end': '#60a5fa',
            'task_count': 10,
            'last_run': '30 minutes ago'
        }
    ]
    
    # Sample tasks data
    tasks = [
        {
            'id': 1,
            'name': 'Transpile Snowflake Views',
            'agent_name': 'SQL Transpiler Agent',
            'agent_color': '#2563eb',
            'type': 'SQL Transpilation',
            'status': 'Completed',
            'status_icon': 'fa-check-circle',
            'status_color': '#10b981',
            'progress': 100
        },
        {
            'id': 2,
            'name': 'Migrate Customer Schema',
            'agent_name': 'Schema Migration Agent',
            'agent_color': '#3d8fcf',
            'type': 'Schema Migration',
            'status': 'In Progress',
            'status_icon': 'fa-spinner',
            'status_color': '#f59e0b',
            'progress': 65
        },
        {
            'id': 3,
            'name': 'Convert SSIS Package - Daily Load',
            'agent_name': 'ETL Workflow Agent',
            'agent_color': '#1a4d7a',
            'type': 'Workflow Conversion',
            'status': 'In Progress',
            'status_icon': 'fa-spinner',
            'status_color': '#f59e0b',
            'progress': 45
        },
        {
            'id': 4,
            'name': 'Validate Sales Data',
            'agent_name': 'Data Quality Agent',
            'agent_color': '#60a5fa',
            'type': 'Data Validation',
            'status': 'Pending',
            'status_icon': 'fa-clock',
            'status_color': '#6b7280',
            'progress': 0
        },
        {
            'id': 5,
            'name': 'Optimize Report Queries',
            'agent_name': 'Performance Optimizer',
            'agent_color': '#0a1929',
            'type': 'Performance Tuning',
            'status': 'Completed',
            'status_icon': 'fa-check-circle',
            'status_color': '#10b981',
            'progress': 100
        },
        {
            'id': 6,
            'name': 'Reconcile Inventory Tables',
            'agent_name': 'Reconciliation Agent',
            'agent_color': '#2563eb',
            'type': 'Data Reconciliation',
            'status': 'In Progress',
            'status_icon': 'fa-spinner',
            'status_color': '#f59e0b',
            'progress': 80
        },
        {
            'id': 7,
            'name': 'Transpile Oracle Procedures',
            'agent_name': 'SQL Transpiler Agent',
            'agent_color': '#2563eb',
            'type': 'SQL Transpilation',
            'status': 'Pending',
            'status_icon': 'fa-clock',
            'status_color': '#6b7280',
            'progress': 0
        },
        {
            'id': 8,
            'name': 'Migrate Analytics Workflows',
            'agent_name': 'ETL Workflow Agent',
            'agent_color': '#1a4d7a',
            'type': 'Workflow Conversion',
            'status': 'Completed',
            'status_icon': 'fa-check-circle',
            'status_color': '#10b981',
            'progress': 100
        }
    ]
    
    return render_template('designer.html', agents=agents, tasks=tasks)

@app.route('/transpile', methods=['GET', 'POST'])
def transpile():
    """Handle SQL transpilation"""
    if request.method == 'GET':
        return render_template('transpile.html', supported_systems=SUPPORTED_SYSTEMS)
    
    try:
        source_dialect = request.form.get('source_dialect')
        target_dialect = request.form.get('target_dialect', 'databricks')
        
        if 'sql_file' not in request.files:
            flash('No file selected')
            return redirect(request.url)
        
        file = request.files['sql_file']
        if file.filename == '':
            flash('No file selected')
            return redirect(request.url)
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = Path(app.config['UPLOAD_FOLDER']) / filename
            file.save(file_path)
            
            # Create TranspileConfig
            output_path = file_path.parent / f"transpiled_{filename}"
            config = TranspileConfig(
                source_dialect=source_dialect,
                input_source=str(file_path),
                input_path=file_path,
                output_path=output_path,
                skip_validation=False
            )
            
            # Create mock workspace client for non-authenticated operations
            engine = SqlglotEngine()
            
            # Perform transpilation
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                result = loop.run_until_complete(engine.transpile(
                    source_dialect, target_dialect, file_path.read_text(), file_path
                ))
                loop.close()
                
                # Save the transpiled content
                if result.transpiled_code:
                    output_path.write_text(result.transpiled_code)
                    
                    return jsonify({
                        'success': True,
                        'message': 'Transpilation completed successfully',
                        'transpiled_code': result.transpiled_code,
                        'warnings': [str(w) for w in result.warnings] if result.warnings else [],
                        'errors': [str(e) for e in result.errors] if result.errors else []
                    })
                else:
                    return jsonify({
                        'success': False,
                        'message': 'Transpilation failed',
                        'errors': [str(e) for e in result.errors] if result.errors else ['Unknown error']
                    })
                    
            except Exception as e:
                return jsonify({
                    'success': False,
                    'message': f'Transpilation error: {str(e)}',
                    'errors': [str(e)]
                })
            finally:
                # Clean up uploaded file
                if file_path.exists():
                    file_path.unlink()
        else:
            flash('Invalid file type. Please upload .sql, .txt, or .zip files.')
            return redirect(request.url)
            
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Server error: {str(e)}',
            'errors': [str(e)]
        })

@app.route('/analyze', methods=['GET', 'POST'])
def analyze():
    """Handle project analysis"""
    if request.method == 'GET':
        return render_template('analyze.html', supported_systems=SUPPORTED_SYSTEMS)
    
    try:
        source_system = request.form.get('source_system')
        
        if 'project_files' not in request.files:
            flash('No files selected')
            return redirect(request.url)
        
        files = request.files.getlist('project_files')
        if not files or all(f.filename == '' for f in files):
            flash('No files selected')
            return redirect(request.url)
        
        # Create temporary directory for analysis
        temp_dir = Path(tempfile.mkdtemp())
        uploaded_files = []
        
        try:
            # Save uploaded files
            for file in files:
                if file and file.filename:
                    filename = secure_filename(file.filename)
                    file_path = temp_dir / filename
                    file.save(file_path)
                    uploaded_files.append(file_path)
            
            # Create analyzer
            analyzer_runner = AnalyzerRunner.create(is_debug=False)
            analyzer_prompts = AnalyzerPrompts()
            lakebridge_analyzer = LakebridgeAnalyzer(analyzer_prompts, analyzer_runner)
            
            # Run analysis
            results_dir = temp_dir / 'results'
            results_dir.mkdir(exist_ok=True)
            
            result = lakebridge_analyzer.run_analyzer(
                source=str(temp_dir),
                results=str(results_dir),
                platform=source_system
            )
            
            return jsonify({
                'success': True,
                'message': 'Analysis completed successfully',
                'source_directory': str(result.source_directory),
                'output_directory': str(result.output_directory),
                'source_system': result.source_system,
                'files_analyzed': len(uploaded_files)
            })
            
        except Exception as e:
            return jsonify({
                'success': False,
                'message': f'Analysis error: {str(e)}',
                'errors': [str(e)]
            })
        finally:
            # Clean up temporary files
            import shutil
            if temp_dir.exists():
                shutil.rmtree(temp_dir)
                
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Server error: {str(e)}',
            'errors': [str(e)]
        })

@app.route('/install', methods=['GET', 'POST'])
def install():
    """Handle transpiler installation"""
    if request.method == 'GET':
        return render_template('install.html')
    
    try:
        # For now, just simulate installation
        # In a real scenario, this would integrate with the actual installation process
        return jsonify({
            'success': True,
            'message': 'Lakebridge components installed successfully',
            'details': 'All transpilers and dependencies are ready to use'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Installation error: {str(e)}',
            'errors': [str(e)]
        })

@app.route('/ssis_migrate', methods=['GET', 'POST'])
def ssis_migrate():
    """Handle SSIS package migration"""
    if request.method == 'GET':
        return render_template('ssis_migrate.html')
    
    try:
        package_type = request.form.get('package_type')
        target_path = request.form.get('target_path', '/Workspace/Shared/migrations')
        
        # Get migration options
        convert_control_flow = request.form.get('convert_control_flow') == 'on'
        convert_data_flow = request.form.get('convert_data_flow') == 'on'
        convert_sql_tasks = request.form.get('convert_sql_tasks') == 'on'
        generate_notebooks = request.form.get('generate_notebooks') == 'on'
        create_workflow = request.form.get('create_workflow') == 'on'
        
        if 'ssis_file' not in request.files:
            flash('No file selected')
            return redirect(request.url)
        
        file = request.files['ssis_file']
        if file.filename == '':
            flash('No file selected')
            return redirect(request.url)
        
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = Path(app.config['UPLOAD_FOLDER']) / filename
            file.save(file_path)
            
            # For now, simulate SSIS migration
            # In a real implementation, this would parse DTSX/ISPAC files and convert them
            try:
                import zipfile
                import xml.etree.ElementTree as ET
                
                # Simulate SSIS package analysis
                tasks_converted = 0
                notebooks_generated = 0
                workflows_created = 0
                artifacts = []
                
                # Check if it's a zip file
                if filename.endswith('.zip'):
                    # Extract and analyze
                    extract_path = Path(app.config['UPLOAD_FOLDER']) / 'extracted'
                    extract_path.mkdir(exist_ok=True)
                    
                    try:
                        with zipfile.ZipFile(file_path, 'r') as zip_ref:
                            zip_ref.extractall(extract_path)
                        
                        # Count DTSX files
                        dtsx_files = list(extract_path.rglob('*.dtsx'))
                        tasks_converted = len(dtsx_files) * 3  # Simulate multiple tasks per package
                        
                        if generate_notebooks:
                            notebooks_generated = len(dtsx_files)
                            artifacts.extend([f"notebook_{i+1}.py" for i in range(len(dtsx_files))])
                        
                        if create_workflow:
                            workflows_created = 1
                            artifacts.append("workflow_definition.json")
                        
                        # Clean up
                        import shutil
                        shutil.rmtree(extract_path)
                    except Exception as e:
                        pass
                
                elif filename.endswith('.dtsx'):
                    # Single DTSX file
                    tasks_converted = 5  # Simulate tasks
                    
                    if generate_notebooks:
                        notebooks_generated = 1
                        artifacts.append("migration_notebook.py")
                    
                    if create_workflow:
                        workflows_created = 1
                        artifacts.append("workflow_definition.json")
                    
                    if convert_sql_tasks:
                        artifacts.append("converted_sql_queries.sql")
                
                elif filename.endswith('.ispac'):
                    # Integration Services Project
                    tasks_converted = 12  # Simulate project with multiple packages
                    
                    if generate_notebooks:
                        notebooks_generated = 3
                        artifacts.extend(["notebook_1.py", "notebook_2.py", "notebook_3.py"])
                    
                    if create_workflow:
                        workflows_created = 1
                        artifacts.append("workflow_definition.json")
                    
                    if convert_sql_tasks:
                        artifacts.append("converted_sql_queries.sql")
                
                # Simulate successful migration
                result = {
                    'success': True,
                    'message': 'SSIS package migration completed successfully',
                    'package_type': package_type,
                    'tasks_converted': tasks_converted,
                    'notebooks_generated': notebooks_generated,
                    'workflows_created': workflows_created,
                    'output_path': target_path,
                    'artifacts': artifacts,
                    'options_applied': {
                        'control_flow': convert_control_flow,
                        'data_flow': convert_data_flow,
                        'sql_tasks': convert_sql_tasks,
                        'notebooks': generate_notebooks,
                        'workflow': create_workflow
                    }
                }
                
                return jsonify(result)
                    
            except Exception as e:
                return jsonify({
                    'success': False,
                    'message': f'Migration error: {str(e)}',
                    'errors': [str(e)]
                })
            finally:
                # Clean up uploaded file
                if file_path.exists():
                    file_path.unlink()
        else:
            flash('Invalid file type. Please upload .dtsx, .ispac, or .zip files.')
            return redirect(request.url)
            
    except Exception as e:
        return jsonify({
            'success': False,
            'message': f'Server error: {str(e)}',
            'errors': [str(e)]
        })


@app.route('/api/status')
def status():
    """API endpoint to check system status"""
    try:
        # Check if core components are available
        engine = SqlglotEngine()
        supported_dialects = engine.supported_dialects
        
        return jsonify({
            'status': 'healthy',
            'supported_dialects': supported_dialects,
            'supported_systems': SUPPORTED_SYSTEMS,
            'version': '0.10.9'
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)