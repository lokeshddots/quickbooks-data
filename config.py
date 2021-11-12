import os
import sys

from dotenv import load_dotenv

base_dir = os.path.abspath(sys.path[0])

config_file_loaded = load_dotenv(dotenv_path=os.path.join(base_dir, '.env'))

if not config_file_loaded:
    raise FileNotFoundError('Missing configuration files.')

# Variables
DATABASE_URL = os.getenv('DATABASE_URL')
DATABASE_NAME = os.getenv('DATABASE_NAME')
