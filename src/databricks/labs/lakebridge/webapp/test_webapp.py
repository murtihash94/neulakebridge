"""
Basic test for the Lakebridge Web Application
"""

import tempfile
import os
from pathlib import Path

def test_webapp_import():
    """Test that the webapp can be imported without errors"""
    try:
        import sys
        webapp_path = Path(__file__).parent
        sys.path.insert(0, str(webapp_path))
        
        from app import app
        assert app is not None
        print("✅ Webapp import test passed")
        return True
    except Exception as e:
        print(f"❌ Webapp import test failed: {e}")
        return False

def test_supported_systems():
    """Test that supported systems are properly configured"""
    try:
        import sys
        webapp_path = Path(__file__).parent
        sys.path.insert(0, str(webapp_path))
        
        from app import SUPPORTED_SYSTEMS
        expected_systems = ['snowflake', 'teradata', 'oracle', 'sqlserver', 'postgresql']
        
        for system in expected_systems:
            assert system in SUPPORTED_SYSTEMS, f"Missing system: {system}"
        
        print(f"✅ Supported systems test passed - {len(SUPPORTED_SYSTEMS)} systems configured")
        return True
    except Exception as e:
        print(f"❌ Supported systems test failed: {e}")
        return False

def test_flask_routes():
    """Test that all required routes are registered"""
    try:
        import sys
        webapp_path = Path(__file__).parent
        sys.path.insert(0, str(webapp_path))
        
        from app import app
        
        # Test that routes exist
        required_routes = ['/', '/transpile', '/analyze', '/install', '/api/status']
        registered_routes = [rule.rule for rule in app.url_map.iter_rules()]
        
        for route in required_routes:
            assert route in registered_routes, f"Missing route: {route}"
        
        print(f"✅ Flask routes test passed - {len(registered_routes)} routes registered")
        return True
    except Exception as e:
        print(f"❌ Flask routes test failed: {e}")
        return False

def test_sqlglot_engine():
    """Test that SQLGlot engine can be imported"""
    try:
        from databricks.labs.lakebridge.transpiler.sqlglot.sqlglot_engine import SqlglotEngine
        engine = SqlglotEngine()
        dialects = engine.supported_dialects
        assert len(dialects) > 0, "No dialects supported"
        print(f"✅ SQLGlot engine test passed - {len(dialects)} dialects supported")
        return True
    except Exception as e:
        print(f"❌ SQLGlot engine test failed: {e}")
        return False

def main():
    """Run all tests"""
    print("🧪 Running Lakebridge Web Application Tests...")
    print()
    
    tests = [
        test_webapp_import,
        test_supported_systems, 
        test_flask_routes,
        test_sqlglot_engine
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        if test():
            passed += 1
        else:
            failed += 1
        print()
    
    print(f"📊 Test Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("🎉 All tests passed! Webapp is ready to use.")
        return True
    else:
        print("⚠️  Some tests failed. Please check the configuration.")
        return False

if __name__ == '__main__':
    main()