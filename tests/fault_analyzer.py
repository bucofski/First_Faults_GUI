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
            nn.Linear(input_size, 256),
            nn.BatchNorm1d(256),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(256, 128),
            nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Dropout(0.2),
            nn.Linear(64, num_classes)
        ).to(self.device)

    def train(self, df: pd.DataFrame, target: str = 'Condition_Message',
              epochs: int = 200, min_samples: int = 5):
        """
        Train to predict faults.

        Args:
            target: Column to predict
            epochs: Training iterations
            min_samples: Only include faults with at least this many occurrences
        """
        # Filter rare faults (hard to learn from 1-2 examples)
        fault_counts = df[target].value_counts()
        common_faults = fault_counts[fault_counts >= min_samples].index
        df_filtered = df[df[target].isin(common_faults)].copy()

        print(f"Filtered: {len(df)} → {len(df_filtered)} records")
        print(f"Classes: {df[target].nunique()} → {len(common_faults)}")

        feature_cols = ['hour', 'day_of_week', 'Level', 'PLC_encoded',
                        'TYPE_encoded', 'Direction_encoded', 'BIT_INDEX']

        X = df_filtered[feature_cols].fillna(0).values
        y = self.label_encoder.fit_transform(df_filtered[target])

        X = self.scaler.fit_transform(X)

        self.build_model(X.shape[1], len(self.label_encoder.classes_))

        X_t = torch.FloatTensor(X).to(self.device)
        y_t = torch.LongTensor(y).to(self.device)

        criterion = nn.CrossEntropyLoss()
        optimizer = torch.optim.Adam(self.model.parameters(), lr=0.001)
        scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, patience=20)

        best_accuracy = 0
        for epoch in range(epochs):
            self.model.train()
            optimizer.zero_grad()
            outputs = self.model(X_t)
            loss = criterion(outputs, y_t)
            loss.backward()
            optimizer.step()
            scheduler.step(loss.detach())  # Add .detach() here

            if epoch % 20 == 0:
                accuracy = (outputs.argmax(1) == y_t).float().mean()
                if accuracy > best_accuracy:
                    best_accuracy = accuracy
                print(f"Epoch {epoch}: Loss={loss.item():.4f}, Accuracy={accuracy:.2%}")

        print(f"\nBest accuracy: {best_accuracy:.2%}")

    def predict_next_fault(self, hour: int, day_of_week: int, level: int,
                           plc_encoded: int, type_encoded: int,
                           direction_encoded: int, bit_index: int,
                           top_k: int = 5) -> pd.DataFrame:
        """Predict most likely next faults with probabilities."""
        self.model.eval()

        features = np.array([[hour, day_of_week, level, plc_encoded,
                              type_encoded, direction_encoded, bit_index]])
        features = self.scaler.transform(features)

        with torch.no_grad():
            X_t = torch.FloatTensor(features).to(self.device)
            outputs = torch.softmax(self.model(X_t), dim=1)
            probs, indices = torch.topk(outputs, top_k)

        predictions = []
        for prob, idx in zip(probs[0], indices[0]):
            fault_name = self.label_encoder.inverse_transform([idx.item()])[0]
            predictions.append({
                'Condition': fault_name,
                'Probability': f"{prob.item():.1%}"
            })

        return pd.DataFrame(predictions)

    def predict_from_current_state(self, df: pd.DataFrame, top_k: int = 5) -> pd.DataFrame:
        """Predict next likely faults based on the most recent data."""
        latest = df.iloc[-1]

        return self.predict_next_fault(
            hour=latest['hour'],
            day_of_week=latest['day_of_week'],
            level=latest['Level'],
            plc_encoded=latest['PLC_encoded'],
            type_encoded=latest['TYPE_encoded'],
            direction_encoded=latest['Direction_encoded'],
            bit_index=latest['BIT_INDEX'],
            top_k=top_k
        )

    def save_model(self, path: str = 'interlock_model.pth'):
        """Save trained model to file."""
        torch.save({
            'model_state': self.model.state_dict(),
            'scaler': self.scaler,
            'label_encoder': self.label_encoder
        }, path)
        print(f"Model saved to {path}")

    def load_model(self, path: str = 'interlock_model.pth', input_size: int = 7):
        """Load trained model from file."""
        checkpoint = torch.load(path, map_location=self.device)
        self.scaler = checkpoint['scaler']
        self.label_encoder = checkpoint['label_encoder']

        num_classes = len(self.label_encoder.classes_)
        self.build_model(input_size, num_classes)
        self.model.load_state_dict(checkpoint['model_state'])
        self.model.eval()
        print(f"Model loaded from {path}")

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