import torch
import torch.nn as nn
import numpy as np
import pandas as pd
from collections import Counter
from sklearn.preprocessing import LabelEncoder, StandardScaler

# Set random seed for reproducibility
RANDOM_SEED = 42
torch.manual_seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)


class InterlockPredictor:
    """Predict which interlock/fault is likely to occur next."""

    def __init__(self):
        self.device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
        self.label_encoder = LabelEncoder()
        self.scaler = StandardScaler()
        self.model = None
        self.mnemonic_to_message: dict[str, str] = {}

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

    def train(self, df: pd.DataFrame, target: str = 'Condition_Mnemonic',
              epochs: int = 200, min_samples: int = 5):
        """
        Train to predict faults.

        Args:
            target: Column to predict
            epochs: Training iterations
            min_samples: Only include faults with at least this many occurrences
        """
        df = df.copy()

        if "Condition_Mnemonic" not in df.columns:
            raise KeyError("Expected column 'Condition_Mnemonic' in dataframe")

        # Clean mnemonic (this is what you TRAIN on)
        df["Condition_Mnemonic"] = (
            df["Condition_Mnemonic"]
            .astype(str)
            .str.strip()
            .replace({"": pd.NA, "None": pd.NA, "nan": pd.NA})
        )
        df = df[df["Condition_Mnemonic"].notna()].copy()

        # Build mnemonic -> message mapping for DISPLAY ONLY
        self.mnemonic_to_message = {}
        if "Condition_Message" in df.columns:
            msg = (
                df["Condition_Message"]
                .astype(str)
                .str.strip()
                .replace({"": pd.NA, "None": pd.NA, "nan": pd.NA})
            )
            tmp = pd.DataFrame({"mn": df["Condition_Mnemonic"], "msg": msg}).dropna(subset=["mn", "msg"])
            if not tmp.empty:
                self.mnemonic_to_message = (
                    tmp.groupby("mn")["msg"]
                    .agg(lambda s: s.value_counts().index[0])  # most common message per mnemonic
                    .to_dict()
                )

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
            scheduler.step(loss.detach())

            if epoch % 20 == 0:
                accuracy = (outputs.argmax(1) == y_t).float().mean()
                if accuracy > best_accuracy:
                    best_accuracy = accuracy
                print(f"Epoch {epoch}: Loss={loss.item():.4f}, Accuracy={accuracy:.2%}")

        print(f"\nBest accuracy: {best_accuracy:.2%}")

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

    def predict_next_fault(self, hour: int, day_of_week: int, level: int,
                           plc_encoded: int, type_encoded: int,
                           direction_encoded: int, bit_index: int,
                           top_k: int = 5) -> pd.DataFrame:
        """Predict most likely next faults with probabilities."""
        if self.model is None:
            raise RuntimeError("Model is not built. Call train() or load_model() first.")

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
            mnemonic = self.label_encoder.inverse_transform([idx.item()])[0]
            predictions.append({
                "Condition_Mnemonic": mnemonic,
                "Condition_Message": self.mnemonic_to_message.get(mnemonic),  # display only
                "Probability": f"{prob.item():.1%}",
            })

        return pd.DataFrame(predictions)

    def save_model(self, path: str = 'interlock_model.pth'):
        """Save trained model to file."""
        torch.save({
            'model_state': self.model.state_dict(),
            'scaler': self.scaler,
            'label_encoder': self.label_encoder,
            'mnemonic_to_message': self.mnemonic_to_message,  # keep display mapping
        }, path)
        print(f"Model saved to {path}")

    def load_model(self, path: str = 'interlock_model.pth', input_size: int = 7):
        """Load trained model from file."""
        checkpoint = torch.load(path, map_location=self.device)
        self.scaler = checkpoint['scaler']
        self.label_encoder = checkpoint['label_encoder']
        self.mnemonic_to_message = checkpoint.get('mnemonic_to_message', {})

        num_classes = len(self.label_encoder.classes_)
        self.build_model(input_size, num_classes)
        self.model.load_state_dict(checkpoint['model_state'])
        self.model.eval()
        print(f"Model loaded from {path}")

    def validate(self, df: pd.DataFrame, sample_size: int = 20) -> pd.DataFrame:
        """
        Validate model predictions against actual data.

        Shows random samples with actual vs predicted fault.
        """
        self.model.eval()

        feature_cols = ['hour', 'day_of_week', 'Level', 'PLC_encoded',
                        'TYPE_encoded', 'Direction_encoded', 'BIT_INDEX']

        # Filter to known classes only
        known_mask = df['Condition_Mnemonic'].isin(self.label_encoder.classes_)
        df_valid = df[known_mask].copy()

        # Fixed seed for reproducible sampling
        sample = df_valid.sample(min(sample_size, len(df_valid)), random_state=RANDOM_SEED)

        # Process all samples at once (batch) instead of one by one
        X = sample[feature_cols].fillna(0).values
        X_scaled = self.scaler.transform(X)

        with torch.no_grad():
            X_t = torch.FloatTensor(X_scaled).to(self.device)
            outputs = torch.softmax(self.model(X_t), dim=1)
            all_probs, all_indices = torch.topk(outputs, 5)

        results = []
        for i, (_, row) in enumerate(sample.iterrows()):
            top5_faults = [self.label_encoder.inverse_transform([idx.item()])[0] for idx in all_indices[i]]
            top5_probs = [f"{p.item():.1%}" for p in all_probs[i]]

            actual = row['Condition_Mnemonic']
            predicted = top5_faults[0]
            in_top5 = actual in top5_faults

            results.append({
                'Actual': actual[:50] + '...' if len(str(actual)) > 50 else actual,
                'Predicted': predicted[:50] + '...' if len(str(predicted)) > 50 else predicted,
                'Correct': '✓' if actual == predicted else '',
                'In_Top5': '✓' if in_top5 else '',
                'Confidence': top5_probs[0]
            })

        results_df = pd.DataFrame(results)

        # Summary stats
        top1_acc = sum(1 for r in results if r['Correct'] == '✓') / len(results)
        top5_acc = sum(1 for r in results if r['In_Top5'] == '✓') / len(results)

        print(f"\n=== Validation Results ({len(results)} samples) ===")
        print(f"Top-1 Accuracy: {top1_acc:.1%}")
        print(f"Top-5 Accuracy: {top5_acc:.1%}")

        return results_df


class PatternAnalyzer:
    """Analyze patterns in interlock occurrences without deep learning."""

    def __init__(self, df: pd.DataFrame, root_cause_only: bool = True):
        self.df = df.copy()
        self.df['TIMESTAMP'] = pd.to_datetime(self.df['TIMESTAMP'])

        # Use Interlock_Message as fallback when Condition_Mnemonic is null
        self.df['Condition_Mnemonic'] = self.df['Condition_Mnemonic'].fillna(self.df['Interlock_Message'])

        if root_cause_only:
            # Keep only the deepest level (root cause) per interlock chain
            self.df = self.df.loc[
                self.df.groupby('Interlock_Log_ID')['Level'].idxmax()
            ]

    def most_frequent_faults(self, top_n: int = 10) -> pd.DataFrame:
        """Get most frequently occurring faults (count by mnemonic, show message as info)."""
        counts = self.df['Condition_Mnemonic'].value_counts().head(top_n)

        result = pd.DataFrame({
            "Mnemonic": counts.index,
            "Count": counts.values,
        })

        if "Condition_Message" in self.df.columns:
            msg = (
                self.df[["Condition_Mnemonic", "Condition_Message"]]
                .dropna(subset=["Condition_Mnemonic", "Condition_Message"])
                .copy()
            )
            msg["Condition_Message"] = (
                msg["Condition_Message"]
                .astype(str)
                .str.strip()
                .replace({"": pd.NA, "None": pd.NA, "nan": pd.NA})
            )
            msg = msg.dropna(subset=["Condition_Message"])

            if not msg.empty:
                mnemonic_to_message = (
                    msg.groupby("Condition_Mnemonic")["Condition_Message"]
                       .agg(lambda s: s.value_counts().index[0])
                       .to_dict()
                )
                result["Info_Message"] = result["Mnemonic"].map(mnemonic_to_message)
            else:
                result["Info_Message"] = pd.NA
        else:
            result["Info_Message"] = pd.NA

        return result[["Mnemonic", "Info_Message", "Count"]]

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
                (self.df['Condition_Mnemonic'] != row['Condition_Mnemonic'])
                ]['Condition_Mnemonic'].tolist()

            for other in nearby:
                pair = tuple(sorted([row['Condition_Mnemonic'], other]))
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

    def _mnemonic_to_info_map(self) -> dict:
        """Build mnemonic -> representative info text (message) mapping for display only."""
        info_col = None
        if "Condition_Message" in self.df.columns and self.df["Condition_Message"].notna().any():
            info_col = "Condition_Message"
        elif "Interlock_Message" in self.df.columns and self.df["Interlock_Message"].notna().any():
            info_col = "Interlock_Message"

        if info_col is None:
            return {}

        tmp = self.df[["Condition_Mnemonic", info_col]].dropna(subset=["Condition_Mnemonic", info_col]).copy()
        tmp[info_col] = (
            tmp[info_col]
            .astype(str)
            .str.strip()
            .replace({"": pd.NA, "None": pd.NA, "nan": pd.NA})
        )
        tmp = tmp.dropna(subset=[info_col])
        if tmp.empty:
            return {}

        return (
            tmp.groupby("Condition_Mnemonic")[info_col]
               .agg(lambda s: s.value_counts().index[0])
               .to_dict()
        )

    def top_risers(self, days_recent: int = 7, days_previous: int = 30,
                   top_n: int = 10, min_recent_count: int = 3,
                   reference_date: pd.Timestamp | None = None) -> pd.DataFrame:
        """
        Find faults that are increasing in frequency with statistical rigor.
        """
        now = reference_date if reference_date else self.df['TIMESTAMP'].max()
        recent_start = now - pd.Timedelta(days=days_recent)
        previous_start = now - pd.Timedelta(days=days_recent + days_previous)

        recent = self.df[self.df['TIMESTAMP'] >= recent_start]
        previous = self.df[
            (self.df['TIMESTAMP'] >= previous_start) &
            (self.df['TIMESTAMP'] < recent_start)
            ]

        # Group by Mnemonic + PLC to make it unique
        group_cols = ['Condition_Mnemonic', 'PLC']

        recent_counts = recent.groupby(group_cols).size()
        previous_counts = previous.groupby(group_cols).size()

        recent_daily = recent_counts / days_recent
        previous_daily = previous_counts / days_previous

        significant_faults = recent_counts[recent_counts >= min_recent_count].index

        risers = []
        for fault_key in significant_faults:
            mnemonic, plc = fault_key
            recent_rate = recent_daily.get(fault_key, 0)
            previous_rate = previous_daily.get(fault_key, 0)
            recent_count = recent_counts.get(fault_key, 0)
            previous_count = previous_counts.get(fault_key, 0)

            if previous_rate > 0:
                change_pct = ((recent_rate - previous_rate) / previous_rate) * 100
            elif recent_rate > 0:
                change_pct = float('inf')
            else:
                continue

            absolute_change = recent_rate - previous_rate

            if change_pct == float('inf'):
                confidence_score = recent_count * 1000
            else:
                confidence_score = change_pct * np.sqrt(recent_count)

            risers.append({
                'Condition': mnemonic,
                'PLC': plc,
                'Recent_Daily_Avg': round(recent_rate, 2),
                'Previous_Daily_Avg': round(previous_rate, 2),
                'Change_%': round(change_pct, 1) if change_pct != float('inf') else 'NEW',
                'Absolute_Change': round(absolute_change, 2),
                'Recent_Count': recent_count,
                'Previous_Count': previous_count,
                'Confidence_Score': round(confidence_score, 1)
            })

        risers_df = pd.DataFrame(risers)
        if risers_df.empty:
            return risers_df

        risers_df = risers_df.sort_values('Confidence_Score', ascending=False)
        risers_df.insert(0, 'Rank', range(1, len(risers_df) + 1))

        # DISPLAY ONLY: attach an info message column
        info_map = self._mnemonic_to_info_map()
        risers_df.insert(3, "Info_Message", risers_df["Condition"].map(info_map))

        return risers_df.head(top_n)



    def compare_time_periods(self,
                             period1_start: pd.Timestamp,
                             period1_end: pd.Timestamp,
                             period2_start: pd.Timestamp,
                             period2_end: pd.Timestamp,
                             min_count: int = 3) -> pd.DataFrame:
        """
        Compare fault frequencies between any two arbitrary time periods.

        More flexible than top_risers for custom analysis.

        Example:
            # Compare this week vs last week
            analyzer.compare_time_periods(
                period1_start=pd.Timestamp('2024-01-08'),
                period1_end=pd.Timestamp('2024-01-14'),
                period2_start=pd.Timestamp('2024-01-01'),
                period2_end=pd.Timestamp('2024-01-07'),
                min_count=2
            )
        """
        period1 = self.df[
            (self.df['TIMESTAMP'] >= period1_start) &
            (self.df['TIMESTAMP'] <= period1_end)
            ]
        period2 = self.df[
            (self.df['TIMESTAMP'] >= period2_start) &
            (self.df['TIMESTAMP'] <= period2_end)
            ]

        period1_days = (period1_end - period1_start).days + 1
        period2_days = (period2_end - period2_start).days + 1

        p1_counts = period1['Condition_Mnemonic'].value_counts()
        p2_counts = period2['Condition_Mnemonic'].value_counts()

        p1_daily = p1_counts / period1_days
        p2_daily = p2_counts / period2_days

        # Get all faults that appear in either period with min count
        all_faults = set(p1_counts[p1_counts >= min_count].index) | \
                     set(p2_counts[p2_counts >= min_count].index)

        comparison = []
        for fault in all_faults:
            p1_rate = p1_daily.get(fault, 0)
            p2_rate = p2_daily.get(fault, 0)

            if p2_rate > 0:
                change_pct = ((p1_rate - p2_rate) / p2_rate) * 100
            elif p1_rate > 0:
                change_pct = float('inf')
            else:
                continue

            comparison.append({
                'Condition': fault,
                'Period1_Daily': round(p1_rate, 2),
                'Period2_Daily': round(p2_rate, 2),
                'Change_%': round(change_pct, 1) if change_pct != float('inf') else 'NEW',
                'Period1_Count': p1_counts.get(fault, 0),
                'Period2_Count': p2_counts.get(fault, 0)
            })

        df = pd.DataFrame(comparison)
        if df.empty:
            return df

        df['_sort'] = df['Change_%'].apply(lambda x: float('inf') if x == 'NEW' else x)
        df = df.sort_values('_sort', ascending=False).drop('_sort', axis=1)

        return df

    def top_risers_with_context(self, days_recent: int = 7, days_previous: int = 30,
                                top_n: int = 10) -> dict:
        """
        Enhanced top_risers that includes context and metadata.

        Returns a dictionary with:
        - risers_df: The main results
        - analysis_period: Date ranges analyzed
        - total_faults_recent: Total faults in recent period
        - total_faults_previous: Total faults in previous period
        - data_quality: Warnings about data quality
        """
        now = self.df['TIMESTAMP'].max()
        recent_start = now - pd.Timedelta(days=days_recent)
        previous_start = now - pd.Timedelta(days=days_recent + days_previous)

        recent = self.df[self.df['TIMESTAMP'] >= recent_start]
        previous = self.df[
            (self.df['TIMESTAMP'] >= previous_start) &
            (self.df['TIMESTAMP'] < recent_start)
            ]

        risers_df = self.top_risers(days_recent, days_previous, top_n)

        warnings = []
        if len(recent) < 10:
            warnings.append(f"Low recent data: only {len(recent)} records")
        if len(previous) < 10:
            warnings.append(f"Low historical data: only {len(previous)} records")

        return {
            'risers_df': risers_df,
            'analysis_period': {
                'recent': f"{recent_start.date()} to {now.date()}",
                'previous': f"{previous_start.date()} to {recent_start.date()}"
            },
            'total_faults_recent': len(recent),
            'total_faults_previous': len(previous),
            'unique_faults_recent': recent['Condition_Mnemonic'].nunique(),
            'unique_faults_previous': previous['Condition_Mnemonic'].nunique(),
            'data_quality_warnings': warnings if warnings else ['No issues detected']
        }