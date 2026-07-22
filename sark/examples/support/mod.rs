pub fn redis_config() -> Result<cartel_redis::Config, cartel_redis::ConfigError> {
    cartel_redis::Config::new(cartel_redis::Capacities {
        connection: 4,
        waiters: 2048,
        inflight: 2048,
        request_entries: 2048,
        request_bytes: 4 * 1024,
        response_bytes: 64 * 1024 * 1024,
        response_values: 65_536,
        max_frame_bytes: 16 * 1024 * 1024,
    })
}
