# anomaly_detector.py
import torch
import torch.nn as nn
import numpy as np


class LSTMAutoencoder(nn.Module):
    """LSTM Autoencoder for anomaly detection."""

    def __init__(self, n_features: int, hidden_size: int = 64):
        super().__init__()

        # Encoder
        self.encoder_lstm = nn.LSTM(n_features, hidden_size, batch_first=True)
        self.encoder_lstm2 = nn.LSTM(hidden_size, hidden_size // 2, batch_first=True)

        # Decoder
        self.decoder_lstm = nn.LSTM(hidden_size // 2, hidden_size, batch_first=True)
        self.decoder_lstm2 = nn.LSTM(hidden_size, hidden_size, batch_first=True)
        self.output_layer = nn.Linear(hidden_size, n_features)

    def forward(self, x):
        # Encode
        x, _ = self.encoder_lstm(x)
        x, (hidden, cell) = self.encoder_lstm2(x)

        # Decode
        x, _ = self.decoder_lstm(x)
        x, _ = self.decoder_lstm2(x)
        x = self.output_layer(x)

        return x


class AnomalyDetector:
    def __init__(self, n_features: int, sequence_length: int):
        self.device = torch.device("mps" if torch.backends.mps.is_available() else "cpu")
        self.model = LSTMAutoencoder(n_features).to(self.device)
        self.sequence_length = sequence_length
        self.threshold = None
        self.criterion = nn.MSELoss()
        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=0.001)

    def create_sequences(self, data: np.ndarray) -> np.ndarray:
        """Convert data into sequences for LSTM."""
        sequences = []
        for i in range(len(data) - self.sequence_length + 1):
            sequences.append(data[i:i + self.sequence_length])
        return np.array(sequences)

    def train(self, data: np.ndarray, epochs: int = 50):
        """Train on normal data to learn normal patterns."""
        sequences = self.create_sequences(data)
        X_tensor = torch.FloatTensor(sequences).to(self.device)

        for epoch in range(epochs):
            self.model.train()
            self.optimizer.zero_grad()

            outputs = self.model(X_tensor)
            loss = self.criterion(outputs, X_tensor)
            loss.backward()
            self.optimizer.step()

            if epoch % 10 == 0:
                print(f"Epoch {epoch}: Loss = {loss.item():.6f}")

        # Set threshold based on training reconstruction errors
        self.model.eval()
        with torch.no_grad():
            reconstructed = self.model(X_tensor)
            mse = torch.mean((X_tensor - reconstructed) ** 2, dim=(1, 2))
            self.threshold = torch.quantile(mse, 0.95).item()

        print(f"Anomaly threshold set to: {self.threshold:.6f}")

    def detect(self, data: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        """Detect anomalies in new data."""
        sequences = self.create_sequences(data)
        X_tensor = torch.FloatTensor(sequences).to(self.device)

        self.model.eval()
        with torch.no_grad():
            reconstructed = self.model(X_tensor)
            mse = torch.mean((X_tensor - reconstructed) ** 2, dim=(1, 2))

        errors = mse.cpu().numpy()
        is_anomaly = errors > self.threshold

        return is_anomaly, errors