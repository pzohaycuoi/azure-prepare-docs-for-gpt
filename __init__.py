import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
scripts_dir = os.path.join(current_dir, 'scripts/')
sys.path.append(current_dir)
sys.path.append(scripts_dir)
