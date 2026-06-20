use std::sync::Arc;
use tracing_appender::rolling;

pub struct LoggerHandle {
    pub _writer: Arc<tracing_appender::non_blocking::NonBlocking>,
    pub _guard: tracing_appender::non_blocking::WorkerGuard,
}

pub fn parse_log_level(level: &str) -> Option<tracing::Level> {
    match level.to_ascii_lowercase().as_str() {
        "trace" => Some(tracing::Level::TRACE),
        "debug" => Some(tracing::Level::DEBUG),
        "info" => Some(tracing::Level::INFO),
        "warn" | "warning" => Some(tracing::Level::WARN),
        "error" => Some(tracing::Level::ERROR),
        "off" | "none" | "" => None,
        _ => Some(tracing::Level::WARN),
    }
}

pub fn init_logger(log_file_path: &str, max_level: Option<tracing::Level>) -> LoggerHandle {
    let Some(max_level) = max_level else {
        tracing_subscriber::fmt()
            .with_writer(std::io::sink)
            .with_max_level(tracing::Level::ERROR)
            .init();
        let (non_blocking, guard) = tracing_appender::non_blocking(std::io::sink());
        return LoggerHandle {
            _writer: Arc::new(non_blocking),
            _guard: guard,
        };
    };

    if log_file_path.trim().is_empty() {
        tracing_subscriber::fmt()
            .with_writer(std::io::sink)
            .with_max_level(max_level)
            .init();
        let (non_blocking, guard) = tracing_appender::non_blocking(std::io::sink());
        return LoggerHandle {
            _writer: Arc::new(non_blocking),
            _guard: guard,
        };
    }

    let file_appender = rolling::never(".", log_file_path);
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::fmt()
        .with_writer(non_blocking.clone())
        .with_max_level(max_level)
        .init();

    LoggerHandle {
        _writer: Arc::new(non_blocking),
        _guard: guard,
    }
}
