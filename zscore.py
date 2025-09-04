import numpy as np

def detect_zscore_anomaly(value, mean, std, threshold=3.0):
    """
    Detect anomaly using Z-score method.

    Args:
        value (float): incoming data point
        mean (float): mean of the feature
        std (float): standard deviation of the feature
        threshold (float): z-score threshold, default 3.0

    Returns:
        bool: True if anomaly, False otherwise
        float: computed z-score
    """
    if std == 0:
        return False, 0.0

    z = (value - mean) / std
    return abs(z) > threshold, z
