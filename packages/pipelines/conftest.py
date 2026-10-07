"""Pytest configuration — add src/ to sys.path for the src-layout."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))
