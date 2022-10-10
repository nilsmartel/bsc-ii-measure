use anyhow::Result;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};

/// returns (User, Password, Database)
fn get_credentials() -> Result<[String; 3]> {
    use std::env::var;

    Ok([
        var("DATABASE_USER")?,
        var("DATABASE_PASSWORD")?,
        var("DATABASE_DB")?,
    ])
}

// example string:
//  "postgresql://dboperator:operatorpass123@localhost:5243/postgres"
fn get_config_str([user, password, database]: [String; 3]) -> String {
    format!("postgresql://{user}:{password}@localhost/{database}")
}

pub fn client_config() -> String {
    get_credentials()
        .map(get_config_str)
        .expect("to read credentials for database")
}

pub fn sqlx_pool() -> Pool<Postgres> {
    let cfg = client_config();

    let pool = PgPoolOptions::new().max_connections(2).connect(&cfg);

    let pool = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(pool);

    pool.expect("set up sqlx postgres pool")
}
