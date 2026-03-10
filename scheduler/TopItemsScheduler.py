import sys
import os
# Get the absolute path of the root directory
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Add the root directory to sys.path
sys.path.insert(0, ROOT_DIR)

import threading
print("import threading")
import time

print("import time")
from models.Items import Items
print("import Items")

from globals import app
with app.app_context():
  try:
    # Log a test message
    print("ECS Scheduler task has started successfully.")
    print("starting thread")
    thread = threading.Thread(target=Items.generating_top_items)
    thread.start()
    print("after starting thread")
    time.sleep(5)
    # Log completion
    print("ECS Scheduler task completed successfully.")
  except Exception as e:
    print(f"An error occurred: {str(e)}")