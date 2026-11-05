"""Local inspection of the generator's source contract (no Spark required)."""
import json
from pathlib import Path

path = Path(__file__).resolve().parents[1] / "docs" / "SOURCE_CONTRACTS_REFERENCE.json"
data = json.loads(path.read_text(encoding="utf-8"))
print(f"Source contracts: {len(data['sources'])} entities, {len(data['snapshot_steps'])} steps")
for i, entities in enumerate(data["snapshot_steps"], start=1):
    print(f"{i:>2}: {', '.join(entities)}")
