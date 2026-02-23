# main.py
from ml_pipeline import load_interlock_data, prepare_features
from fault_analyzer import PatternAnalyzer, InterlockPredictor
from snapshot_manager import TrendSnapshotManager

# Load data
df = load_interlock_data(top_n=100000)
df = prepare_features(df)

# === Pattern Analysis ===
analyzer = PatternAnalyzer(df)

print("=== Most Frequent Faults ===")
print(analyzer.most_frequent_faults(10))

print("\n=== Faults by PLC ===")
print(analyzer.faults_by_plc())

print("\n=== Faults by Hour ===")
print(analyzer.faults_by_hour())

print("\n=== Correlated Faults (occur together) ===")
print(analyzer.find_correlated_faults(time_window_minutes=5))

print("\n=== Anomaly Days (unusual spike in faults) ===")
print(analyzer.detect_anomalies())

# === NEW: Top Risers with Context ===
print("\n=== Top Risers (increasing faults) WITH CONTEXT ===")
result = analyzer.top_risers_with_context(days_recent=14, days_previous=30, top_n=10)
print(f"\nAnalyzing periods:")
print(f"  Recent: {result['analysis_period']['recent']}")
print(f"  Previous: {result['analysis_period']['previous']}")
print(f"\nData points:")
print(f"  Recent faults: {result['total_faults_recent']}")
print(f"  Previous faults: {result['total_faults_previous']}")
print(f"  Unique faults (recent): {result['unique_faults_recent']}")
print(f"  Unique faults (previous): {result['unique_faults_previous']}")
print(f"\nQuality: {', '.join(result['data_quality_warnings'])}")
print("\nTop Risers:")
print(result['risers_df'].to_string(index=False))

# === Optional: Compare specific time periods ===
# Uncomment to compare two specific weeks
# print("\n=== Custom Period Comparison ===")
# import pandas as pd
# comparison = analyzer.compare_time_periods(
#     period1_start=pd.Timestamp('2024-12-01'),
#     period1_end=pd.Timestamp('2024-12-07'),
#     period2_start=pd.Timestamp('2024-11-01'),
#     period2_end=pd.Timestamp('2024-11-07'),
#     min_count=2
# )
# print(comparison.to_string(index=False))

# === Train Predictor ===
predictor = InterlockPredictor()
predictor.train(df, target="Condition_Mnemonic", epochs=1000, min_samples=5)

# === Validate predictions ===
print("\n=== Model Validation ===")
validation_results = predictor.validate(df, sample_size=20)
print(validation_results.to_string())

# === Save and predict ===
predictor.save_model('interlock_model.pth')

print("\n=== Predicted Next Faults ===")
print(predictor.predict_from_current_state(df, top_k=10))

print("\n=== Prediction for Monday 8 AM ===")
print(predictor.predict_next_fault(
    hour=8,
    day_of_week=0,
    level=1,
    plc_encoded=0,
    type_encoded=0,
    direction_encoded=0,
    bit_index=0,
    top_k=5
))

print("\n=== DEBUG INFO ===")
print(f"DataFrame PLC values: {df['PLC'].unique()[:5]}")
print(f"DataFrame Condition_Mnemonic sample: {df['Condition_Mnemonic'].head(3).tolist()}")

risers = analyzer.top_risers(days_recent=7, days_previous=30, top_n=5)
print(f"\nTop risers columns: {risers.columns.tolist()}")
print(f"Top risers sample:\n{risers[['Condition', 'PLC']].head()}")
snapshot_mgr = TrendSnapshotManager()

print("\n=== Saving Trend Snapshot ===")
snapshot_mgr.save_snapshot(analyzer, days_recent=7, days_previous=30)