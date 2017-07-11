package io.hentitydb.store;

/**
 * A health check for a component of your application.
 */
public interface HealthCheck {

    String getName();

    Result check();

    /**
     * The result of a {@link HealthCheck} being run. It can be healthy (with an optional message)
     * or unhealthy (with either an error message or a thrown exception).
     */
    class Result {
        private final boolean healthy;
        private final String message;
        private final Throwable error;

        public Result(boolean healthy) {
            this.healthy = healthy;
            this.message = null;
            this.error = null;
        }

        public Result(boolean healthy, String message) {
            this.healthy = healthy;
            this.message = message;
            this.error = null;
        }

        public Result(boolean healthy, String message, Throwable error) {
            this.healthy = healthy;
            this.message = message;
            this.error = error;
        }

        public boolean isHealthy() {
            return healthy;
        }

        public String getMessage() {
            return message;
        }

        public Throwable getError() {
            return error;
        }
    }
}
