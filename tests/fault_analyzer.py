# fault_analyzer.py
import torch
import torch.nn as nn
import numpy as np
import pandas as pd
from collections import Counter
from sklearn.preprocessing import LabelEncoder, StandardScaler


class InterlockPredictor:
    """Predict which interlock/fault is likely to occur next."""

    def __init__(self):
        self.device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
        self.label_encoder = LabelEncoder()
        self.scaler = StandardScaler()
        self.model = None

    def build_model(self, input_size: int, num_classes: int):
        self.model = nn.Sequential(
            nn.Linear(input_size, 128),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(64, num_classes)
        ).to(self.device)

    def train(self, df: pd.DataFrame, epochs: int = 100):
        """Train to predict next likely interlock based on patterns."""
        # Features: time patterns, PLC, level, type
        feature_cols = ['hour', 'day_of_week', 'Level', 'PLC_encoded',
                        'TYPE_encoded', 'Direction_encoded', 'BIT_INDEX']

        X = df[feature_cols].values
        y = self.label_encoder.fit_transform(df['Condition_Message'])

        X = self.scaler.fit_transform(X)

        self.build_model(X.shape[1], len(self.label_encoder.classes_))

        X_t = torch.FloatTensor(X).to(self.device)
        y_t = torch.LongTensor(y).to(self.device)

        criterion = nn.CrossEntropyLoss()
        optimizer = torch.optim.Adam(self.model.parameters(), lr=0.001)

        for epoch in range(epochs):
            self.model.train()
            optimizer.zero_grad()
            outputs = self.model(X_t)
            loss = criterion(outputs, y_t)
            loss.backward()
            optimizer.step()

            if epoch % 20 == 0:
                accuracy = (outputs.argmax(1) == y_t).float().mean()
                print(f"Epoch {epoch}: Loss={loss.item():.4f}, Accuracy={accuracy:.2%}")

    def predict_next_fault(self, plc: str, hour: int, day_of_week: int,
                           level: int, type_encoded: int, direction: int,
                           bit_index: int, top_k: int = 5) -> list[tuple[str, float]]:
        """Predict most likely next faults."""
        self.model.eval()

        # You'd need to encode PLC the same way as training
        features = np.array([[hour, day_of_week, level, 0, type_encoded, direction, bit_index]])
        features = self.scaler.transform(features)

        with torch.no_grad():
            X_t = torch.FloatTensor(features).to(self.device)
            outputs = torch.softmax(self.model(X_t), dim=1)
            probs, indices = torch.topk(outputs, top_k)

        predictions = []
        for prob, idx in zip(probs[0], indices[0]):
            fault_name = self.label_encoder.inverse_transform([idx.item()])[0]
            predictions.append((fault_name, prob.item()))

        return predictions


class PatternAnalyzer:
    """Analyze patterns in interlock occurrences without deep learning."""

    def __init__(self, df: pd.DataFrame, root_cause_only: bool = True):
        self.df = df.copy()
        self.df['TIMESTAMP'] = pd.to_datetime(self.df['TIMESTAMP'])
        # Use Interlock_Message as fallback when Condition_Message is null
        self.df['Condition_Message'] = self.df['Condition_Message'].fillna(self.df['Interlock_Message'])

        if root_cause_only:
            # Keep only the deepest level (root cause) per interlock chain
            self.df = self.df.loc[
                self.df.groupby('Interlock_Log_ID')['Level'].idxmax()
            ]

    def most_frequent_faults(self, top_n: int = 10) -> pd.DataFrame:
        """Get most frequently occurring interlocks."""
        counts = self.df['Condition_Message'].value_counts().head(top_n)
        return pd.DataFrame({'Condition': counts.index, 'Count': counts.values})

    def faults_by_plc(self) -> pd.DataFrame:
        """Count faults per PLC."""
        return self.df.groupby('PLC').size().sort_values(ascending=False).reset_index(name='Count')

    def faults_by_hour(self) -> pd.DataFrame:
        """Analyze fault distribution by hour."""
        self.df['hour'] = self.df['TIMESTAMP'].dt.hour
        return self.df.groupby('hour').size().reset_index(name='Count')

    def find_correlated_faults(self, time_window_minutes: int = 5) -> list[tuple]:
        """Find faults that often occur together within a time window."""
        self.df = self.df.sort_values('TIMESTAMP')
        correlations = []

        for i, row in self.df.iterrows():
            window_start = row['TIMESTAMP']
            window_end = window_start + pd.Timedelta(minutes=time_window_minutes)

            nearby = self.df[
                (self.df['TIMESTAMP'] >= window_start) &
                (self.df['TIMESTAMP'] <= window_end) &
                (self.df['Condition_Message'] != row['Condition_Message'])
                ]['Condition_Message'].tolist()

            for other in nearby:
                pair = tuple(sorted([row['Condition_Message'], other]))
                correlations.append(pair)

        return Counter(correlations).most_common(10)

    def detect_anomalies(self, threshold_std: float = 2.0) -> pd.DataFrame:
        """Detect unusual spikes in fault frequency."""
        daily_counts = self.df.groupby(self.df['TIMESTAMP'].dt.date).size()
        mean_count = daily_counts.mean()
        std_count = daily_counts.std()

        anomaly_threshold = mean_count + (threshold_std * std_count)
        anomalies = daily_counts[daily_counts > anomaly_threshold]

        return pd.DataFrame({
            'Date': anomalies.index,
            'Fault_Count': anomalies.values,
            'Threshold': anomaly_threshold
        })

    def top_risers(self, days_recent: int = 7, days_previous: int = 30, top_n: int = 10) -> pd.DataFrame:
        """
        Find faults that are increasing in frequency.
        
        Compares recent period vs previous period to find trending issues.
        
        Args:
            days_recent: Number of recent days to analyze
            days_previous: Number of previous days to compare against
            top_n: Number of top risers to return
        """
        now = self.df['TIMESTAMP'].max()
        recent_start = now - pd.Timedelta(days=days_recent)
        previous_start = now - pd.Timedelta(days=days_recent + days_previous)
        
        # Split data into periods
        recent = self.df[self.df['TIMESTAMP'] >= recent_start]
        previous = self.df[
            (self.df['TIMESTAMP'] >= previous_start) & 
            (self.df['TIMESTAMP'] < recent_start)
        ]
        
        # Count faults per period
        recent_counts = recent['Condition_Message'].value_counts()
        previous_counts = previous['Condition_Message'].value_counts()
        
        # Normalize by number of days
        recent_daily = recent_counts / days_recent
        previous_daily = previous_counts / days_previous
        
        # Calculate change
        all_faults = set(recent_counts.index) | set(previous_counts.index)
        
        risers = []
        for fault in all_faults:
            recent_rate = recent_daily.get(fault, 0)
            previous_rate = previous_daily.get(fault, 0)
            
            if previous_rate > 0:
                change_pct = ((recent_rate - previous_rate) / previous_rate) * 100
            elif recent_rate > 0:
                change_pct = float('inf')  # New fault
            else:
                continue
                
            risers.append({
                'Condition': fault,
                'Recent_Daily_Avg': round(recent_rate, 2),
                'Previous_Daily_Avg': round(previous_rate, 2),
                'Change_%': round(change_pct, 1) if change_pct != float('inf') else 'NEW',
                'Recent_Count': recent_counts.get(fault, 0),
                'Previous_Count': previous_counts.get(fault, 0)
            })
        
        risers_df = pd.DataFrame(risers)
        
        # Sort by change percentage (NEW faults at top, then by %)
        risers_df['_sort'] = risers_df['Change_%'].apply(
            lambda x: float('inf') if x == 'NEW' else x
        )
        risers_df = risers_df.sort_values('_sort', ascending=False).drop('_sort', axis=1)
        
        return risers_df.head(top_n)