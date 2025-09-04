from collections import deque
import numpy as np

def init_rolling_window(window_size):
    """
    Initialize a rolling window buffer.
    """
    return deque(maxlen=window_size)

def update_and_detect_anomaly(buffer, value, threshold=3.0):
    """
    Update rolling mean buffer and detect anomalies.

    Args:
        buffer (deque): rolling window buffer
        value (float): new incoming data point
        threshold (float): deviation factor from rolling mean

    Returns:
        bool: True if anomaly, False otherwise
        float: rolling mean
    """
    buffer.append(value)
    if len(buffer) < buffer.maxlen:
        return False, np.mean(buffer)  # Not enough data yet

    mean = np.mean(buffer)
    std = np.std(buffer) if np.std(buffer) != 0 else 1e-6
    z = (value - mean) / std
    return abs(z) > threshold, mean
