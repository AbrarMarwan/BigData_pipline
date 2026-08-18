import json
import os

def save_metrics(metrics_data, output_path="reports/results.json"):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(metrics_data, f, ensure_ascii=False, indent=4)
    print(f"\n📊 Metrics successfully exported to: {output_path}")