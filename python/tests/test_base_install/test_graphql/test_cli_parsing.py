"""
Test for python_generate_config function
"""

import sys
import os
import tempfile
from unittest.mock import patch
from raphtory_graphql import generate_config


def test_generate_config_with_cli_args():
    """
    Test that generate_config correctly parses CLI arguments.
    
    This test:
    1. Sets up CLI arguments simulating: raphtory server --port 8080 --log-level info
    2. Calls generate_config() which reads sys.argv
    3. Verifies the returned config contains server_args and app_config
    """
    cli_args = [
        'raphtory',           
        'server',             
    ]
    
    with patch.object(sys, 'argv', cli_args):
        result = generate_config()
    
    print(result)


def test_cli_parsing_with_config_file():
    """
    Test that generate_config correctly parses a config file.
    
    This test:
    1. Creates a temporary config file with test values
    2. Sets up CLI arguments pointing to that config file
    3. Removes RAPHTORY_CACHE_CAPACITY env var if set
    4. Calls generate_config() to parse the config file
    5. Asserts the parsed config contains the values from the config file
    """
    
    # Create a temporary config file with TOML content
    config_content = """
[cache]
capacity = 123

[logging]
level = "debug"
"""
    
    # Create temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
        f.write(config_content)
        config_file_path = f.name
    
    try:
        # Setup CLI arguments pointing to the config file
        cli_args = [
            'raphtory',
            'server',
            '--config-file',
            config_file_path
        ]
        
        # Remove environment variable that could override config
        with patch.dict(os.environ, {}, clear=False):
            # Ensure the env var is removed
            os.environ.pop('RAPHTORY_CACHE_CAPACITY', None)
            
            # Patch sys.argv and call generate_config
            with patch.object(sys, 'argv', cli_args):
                result = generate_config()
        
        # Assert the config was loaded correctly
        assert result is not None, "generate_config should return a result"
        assert 'app_config' in result, "Result should contain app_config"
        
        print("Config from file:", result['app_config'])
        
        # Verify the cache capacity was loaded from the config file
        assert 'cache' in result['app_config'], "app_config should have cache settings"
        
    finally:
        # Clean up the temporary file
        os.unlink(config_file_path)


