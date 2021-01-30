package edu.gmu.cds.cdillon2.results;

public class DoubleResult {
    public final Double out;

    public DoubleResult(Double value) {
        out = value;
    }

    public DoubleResult(Float value) {
        out = value.doubleValue();
    }
}
