use std::error::Error;

use tracing_subscriber::{fmt, prelude::*, EnvFilter, Registry};

pub(crate) fn init() -> Result<(), Box<dyn Error>> {
    // Set env filter level to kvkv=trace
    let env_filter = match EnvFilter::try_from_default_env() {
        Ok(filter) => filter,
        Err(_) => EnvFilter::default().add_directive("kvkv=trace".parse()?),
    };
    
    Registry::default()
        .with(env_filter)
        .with(fmt::layer())
        .init();
    Ok(())
}
