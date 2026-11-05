# In a step's pod every code file sits in one flat directory, so the metrics import each
# other by module name (`from metric_types import ...`). Put blocks/metrics on the path
# the same way here.
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
