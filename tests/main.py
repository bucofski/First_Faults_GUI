# main.py
from ml_pipeline import load_interlock_data, prepare_features
from fault_analyzer import PatternAnalyzer, InterlockPredictor

# Load data
df = load_interlock_data(top_n=20000)
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

print("\n=== Top Risers (increasing faults) ===")
print(analyzer.top_risers(days_recent=7, days_previous=30, top_n=10))

# === Train Predictor FIRST ===
predictor = InterlockPredictor()
predictor.train(df, target='Condition_Message', epochs=500, min_samples=10)

# === Now use it ===
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