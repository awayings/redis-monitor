package com.yj.redis.monitor.alert;

public class AlertRule {

    private String metricName;
    private double threshold;
    private AlertOperator operator;

    public String getMetricName() {
        return metricName;
    }

    public void setMetricName(String metricName) {
        this.metricName = metricName;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public AlertOperator getOperator() {
        return operator;
    }

    public void setOperator(AlertOperator operator) {
        this.operator = operator;
    }
}
