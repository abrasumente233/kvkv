use std::error::Error;

use tracing_subscriber::{fmt, prelude::*, EnvFilter, Registry};

pub(crate) fn init() -> Result<(), Box<dyn Error>> {
    Registry::default()
        .with(EnvFilter::from_default_env())
        .with(fmt::layer())
        .init();
    Ok(())
}
