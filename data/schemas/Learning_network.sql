-- Copy from the first artifact (snapshot_table)
CREATE TABLE fault_trend_snapshots (
    snapshot_id INT PRIMARY KEY AUTO_INCREMENT,
    snapshot_date DATE NOT NULL,
    condition_message VARCHAR(500) NOT NULL,

    -- Analysis parameters
    days_recent INT NOT NULL,
    days_previous INT NOT NULL,

    -- Metrics from analysis
    recent_daily_avg DECIMAL(10,2),
    previous_daily_avg DECIMAL(10,2),
    change_percent DECIMAL(10,2),
    absolute_change DECIMAL(10,2),
    recent_count INT,
    previous_count INT,
    confidence_score DECIMAL(15,2),
    rank_position INT,

    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Indexes
    INDEX idx_snapshot_date (snapshot_date),
    INDEX idx_condition (condition_message),
    INDEX idx_snapshot_condition (snapshot_date, condition_message),

    -- Prevent duplicate snapshots for same date/condition/parameters
    UNIQUE KEY unique_snapshot (snapshot_date, condition_message, days_recent, days_previous)
);

CREATE VIEW fault_trend_history AS
SELECT
    condition_message,
    snapshot_date,
    recent_daily_avg,
    change_percent,
    confidence_score,
    rank_position,
    LAG(recent_daily_avg) OVER (PARTITION BY condition_message ORDER BY snapshot_date) as prev_week_avg,
    recent_daily_avg - LAG(recent_daily_avg) OVER (PARTITION BY condition_message ORDER BY snapshot_date) as week_over_week_change
FROM fault_trend_snapshots
WHERE days_recent = 7 AND days_previous = 30
ORDER BY condition_message, snapshot_date DESC;