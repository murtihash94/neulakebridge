# Lakebridge Web Application

A user-friendly web interface for the Lakebridge migration tool, making it easier to migrate workloads to Databricks by converting SQL and workflow orchestration.

![Lakebridge Logo](src/databricks/labs/lakebridge/webapp/static/lakebridge-lockup.png)

## Features

- **SQL Transpilation**: Convert SQL files from various source systems (Snowflake, Teradata, Oracle, SQL Server, etc.) to Databricks SQL dialect
- **Project Analysis**: Analyze existing workloads and get detailed migration insights
- **Component Installation**: Install and configure Lakebridge transpilers and dependencies
- **File Upload**: Support for uploading SQL files, scripts, and project archives
- **Real-time Results**: View transpilation results and analysis reports instantly
- **Download Options**: Download converted SQL files and analysis reports

## Installation

### Prerequisites

- Python 3.10 or higher
- pip package manager

### Quick Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/murtihash94/neulakebridge.git
   cd neulakebridge
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install the package with web dependencies:**
   ```bash
   pip install -e .
   pip install flask
   ```

### Alternative Installation (from PyPI)

```bash
pip install databricks-labs-lakebridge flask
```

## Running the Web Application

### Method 1: Using the Launcher Script

```bash
cd src/databricks/labs/lakebridge/webapp
python run_webapp.py
```

### Method 2: Direct Flask Launch

```bash
cd src/databricks/labs/lakebridge/webapp
python app.py
```

### Method 3: Using Flask CLI

```bash
cd src/databricks/labs/lakebridge/webapp
export FLASK_APP=app.py
flask run --host=0.0.0.0 --port=5000
```

## Accessing the Web Interface

Once the application is running, open your web browser and navigate to:

```
http://localhost:5000
```

You will see the Lakebridge landing page with options to:

- **Transpile SQL**: Convert individual SQL files
- **Analyze Project**: Upload and analyze multiple project files
- **Install Components**: Set up required Lakebridge components

## Usage Guide

### SQL Transpilation

1. Click on "Transpile SQL" or navigate to `/transpile`
2. Select your source system (e.g., Snowflake, Teradata, Oracle)
3. Upload a SQL file (.sql or .txt)
4. Click "Transpile SQL" to convert
5. View the results and download the converted SQL

### Project Analysis

1. Click on "Analyze Project" or navigate to `/analyze`
2. Select your source system
3. Upload multiple project files (SQL, scripts, archives)
4. Click "Analyze Project" to get insights
5. Review the analysis results and migration recommendations

### Component Installation

1. Click on "Install Components" or navigate to `/install`
2. Click "Install Components" to set up required tools
3. Follow the installation progress and status updates

## Supported Source Systems

- Snowflake
- Teradata
- Oracle
- SQL Server
- PostgreSQL
- MySQL
- Amazon Redshift
- Google BigQuery
- Azure Synapse
- Apache Hive

## File Formats Supported

- `.sql` - SQL script files
- `.txt` - Text files containing SQL
- `.zip` - Compressed project archives
- `.tar` - Tar archives
- `.gz` - Gzip compressed files

## Configuration

### Environment Variables

- `FLASK_SECRET_KEY`: Secret key for Flask sessions (default: 'lakebridge-dev-key')
- `FLASK_ENV`: Flask environment (development/production)
- `UPLOAD_FOLDER`: Directory for temporary file uploads (default: system temp directory)

### Example Configuration

```bash
export FLASK_SECRET_KEY="your-secret-key-here"
export FLASK_ENV="production"
python run_webapp.py
```

## API Endpoints

The web application also provides REST API endpoints:

- `GET /api/status` - Check system status and supported dialects
- `POST /transpile` - Transpile SQL files
- `POST /analyze` - Analyze project files
- `POST /install` - Install components

### Example API Usage

```bash
# Check system status
curl http://localhost:5000/api/status

# Transpile SQL (multipart form)
curl -X POST -F "source_dialect=snowflake" -F "sql_file=@example.sql" http://localhost:5000/transpile
```

## Development

### Setting up Development Environment

1. Clone the repository and install dependencies as above
2. Install development dependencies:
   ```bash
   pip install -r requirements-dev.txt  # if available
   ```
3. Run in debug mode:
   ```bash
   cd src/databricks/labs/lakebridge/webapp
   export FLASK_ENV=development
   python app.py
   ```

### Project Structure

```
src/databricks/labs/lakebridge/webapp/
├── app.py                 # Main Flask application
├── run_webapp.py         # Application launcher
├── templates/            # HTML templates
│   ├── base.html        # Base template with navigation
│   ├── index.html       # Landing page
│   ├── transpile.html   # SQL transpilation page
│   ├── analyze.html     # Project analysis page
│   └── install.html     # Component installation page
└── static/              # Static assets
    └── lakebridge-lockup.png  # Logo image
```

## Troubleshooting

### Common Issues

1. **Module Import Errors**: Ensure you've installed all dependencies and activated your virtual environment
2. **File Upload Errors**: Check file size limits (50MB max) and supported formats
3. **Port Already in Use**: Use a different port with `flask run --port=5001`

### Getting Help

- **Documentation**: [https://databrickslabs.github.io/lakebridge/](https://databrickslabs.github.io/lakebridge/)
- **GitHub Issues**: [https://github.com/databrickslabs/lakebridge/issues](https://github.com/databrickslabs/lakebridge/issues)
- **CLI Documentation**: [https://databrickslabs.github.io/lakebridge/docs/installation/](https://databrickslabs.github.io/lakebridge/docs/installation/)

## License

This project is licensed under the Databricks License. See the LICENSE file for details.

## Contributing

Contributions are welcome! Please read the contributing guidelines and submit pull requests to the main repository.