use std::env::VarError;

/// returns (User, Password, Database)
fn get_credentials() -> Result<[String; 3], VarError> {
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

pub fn client_str() -> String {
    get_credentials()
        .map(get_config_str)
        .expect("to read credentials for database")
}
