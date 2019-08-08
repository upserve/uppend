package com.upserve.uppend.metrics;

public interface InternalMetrics<T extends InternalMetrics<T>> {
    String toString();

    String present(T previous);

    T minus(T other);

    enum Prefix {
        NANO(1e-9d),
        MICRO(1e-6d),
        MILLI(1e-3d),
        NONE(1d),
        KILO(1e3d),
        MEGA(1e6d),
        GIGA(1e9d);

        private double value;

        Prefix(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        public double toNano(double convert) {
            return value * convert / NANO.value;
        }

        public double toMicro(double convert) {
            return value * convert / MICRO.value;
        }

        public double toMilli(double convert) {
            return value * convert / MILLI.value;
        }

        public double toNone(double convert) {
            return value * convert / NONE.value;
        }

        public double toKilo(double convert) {
            return value * convert / KILO.value;
        }

        public double toMega(double convert) {
            return value * convert / MEGA.value;
        }

        public double toGiga(double convert) {
            return value * convert / GIGA.value;
        }
    }
}
