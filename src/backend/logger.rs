use std::sync::Arc;
use tracing_appender::rolling;

pub struct LoggerHandle {
    pub writer: Arc<tracing_appender::non_blocking::NonBlocking>,
    pub _guard: tracing_appender::non_blocking::WorkerGuard,
}

pub fn init_logger(log_file_path: &str) -> LoggerHandle {
    // Rolling::never writes to a single file
    let file_appender = rolling::never(".", log_file_path);
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::fmt()
        .with_writer(non_blocking.clone())
        .with_max_level(tracing::Level::DEBUG)
        .init();

    LoggerHandle {
        writer: Arc::new(non_blocking),
        _guard: guard,
    }
}
