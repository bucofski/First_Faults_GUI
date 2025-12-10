# main.py
from ml_pipeline import load_interlock_data, prepare_features
from fault_analyzer import PatternAnalyzer, InterlockPredictor

# Load data
df = load_interlock_data(top_n=20000)
df = prepare_features(df)

# === Quick Pattern Analysis (no training needed) ===
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

print("\n=== Anomaly Days (unusual spike in faults) ===")
print(analyzer.detect_anomalies())

print("\n=== Top Risers (increasing faults) ===")
print(analyzer.top_risers(days_recent=7, days_previous=30, top_n=10))


# === Deep Learning Prediction (optional) ===
predictor = InterlockPredictor()
#predictor.train(df, epochs=300, min_samples=10)
predictor.train(df, target='Condition_Message', epochs=1000, min_samples=5)