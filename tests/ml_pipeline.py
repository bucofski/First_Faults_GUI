# ml_pipeline.py
import pandas as pd
import numpy as np
from data.repositories.repository import InterlockRepository


def load_interlock_data(top_n: int | None = None) -> pd.DataFrame:
    """Load interlock data from the database."""
    repo = InterlockRepository()
    return repo.get_interlock_chain(top_n=top_n)


def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare features for ML from interlock data."""
    df = df.copy()

    # Time-based features
    df['TIMESTAMP'] = pd.to_datetime(df['TIMESTAMP'])
    df['hour'] = df['TIMESTAMP'].dt.hour
    df['day_of_week'] = df['TIMESTAMP'].dt.dayofweek
    df['minute'] = df['TIMESTAMP'].dt.minute

    # Encode categorical columns
    df['PLC_encoded'] = pd.factorize(df['PLC'])[0]
    df['TYPE_encoded'] = pd.factorize(df['TYPE'])[0]
    df['Status_encoded'] = pd.factorize(df['Status'])[0]
    df['Direction_encoded'] = pd.factorize(df['Direction'])[0]

    return df