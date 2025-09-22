#!/usr/bin/env python3
"""
Lakebridge Web Application Launcher

This script launches the Lakebridge web application, providing a user-friendly
interface for SQL transpilation, project analysis, and component installation.
"""

import sys
import os
from pathlib import Path

# Add the webapp module to the Python path
webapp_dir = Path(__file__).parent
sys.path.insert(0, str(webapp_dir))

try:
    from app import app
    
    def main():
        print("🚀 Starting Lakebridge Web Application...")
        print("📋 Available features:")
        print("   • SQL Transpilation - Convert SQL between different dialects")
        print("   • Project Analysis - Analyze existing workloads")
        print("   • Component Installation - Set up Lakebridge components")
        print("")
        print("🌐 Open your browser and navigate to: http://localhost:5000")
        print("⏹️  Press Ctrl+C to stop the server")
        print("")
        
        # Start the Flask application
        app.run(debug=False, host='0.0.0.0', port=5000)
        
except ImportError as e:
    print(f"❌ Error importing Lakebridge webapp: {e}")
    print("💡 Please ensure you have installed the required dependencies:")
    print("   pip install flask")
    sys.exit(1)
except Exception as e:
    print(f"❌ Error starting Lakebridge webapp: {e}")
    sys.exit(1)

if __name__ == '__main__':
    main()