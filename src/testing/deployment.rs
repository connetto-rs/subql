//! Explicit PostgreSQL roles and caller settings for authorization fixtures.

use alloc::collections::BTreeMap;
use alloc::format;
use alloc::string::{String, ToString};
use sql_traits::structs::{Deployment, ParserDB, SessionSetting, SetPacking, SettingValue};
use sqlparser::{ast::DataType, dialect::PostgreSqlDialect};

/// The fixture's login and acting role.
pub const ROLE: &str = "app_user";

/// The role declaration required by the fixture deployment.
pub const ROLES: &str = "CREATE ROLE app_user LOGIN;";

/// Parse a fixture with its deployment role declared.
///
/// # Panics
/// Panics if the fixture is invalid PostgreSQL DDL.
#[must_use]
pub fn db(sql: &str) -> ParserDB {
    ParserDB::parse::<PostgreSqlDialect>(&format!("{ROLES}\n{sql}"))
        .expect("the authorization fixture parses")
}

/// Declare the fixture's caller settings and any explicit overrides.
///
/// # Panics
/// Panics if the deployment contains invalid declarations.
#[must_use]
pub fn with_settings(settings: impl IntoIterator<Item = SessionSetting>) -> Deployment {
    let mut declared: BTreeMap<String, SessionSetting> = [
        "app.user_id",
        "app.current_user_id",
        "request.jwt.claim.sub",
    ]
    .into_iter()
    .map(|name| (name.to_string(), caller(name)))
    .collect();
    for setting in settings {
        declared.insert(setting.name().to_string(), setting);
    }
    Deployment::new(ROLE, [ROLE], declared.into_values()).expect("a valid fixture deployment")
}

/// Declare the fixture's caller settings.
///
/// # Panics
/// Panics if the fixture deployment is invalid.
#[must_use]
pub fn callers() -> Deployment {
    with_settings([])
}

/// Declare a caller identity rendered as text.
///
/// # Panics
/// Panics if `name` is not a custom setting name.
#[must_use]
pub fn caller(name: &str) -> SessionSetting {
    SessionSetting::value(name, SettingValue::Caller(DataType::Text)).expect("a caller setting")
}

/// Declare a scalar rendered as text.
///
/// # Panics
/// Panics if `name` is not a custom setting name.
#[must_use]
pub fn scalar(name: &str) -> SessionSetting {
    SessionSetting::value(name, SettingValue::Scalar(DataType::Text)).expect("a scalar setting")
}

/// Declare text values delimited by a comma.
///
/// # Panics
/// Panics if `name` is not a custom setting name.
#[must_use]
pub fn set(name: &str) -> SessionSetting {
    SessionSetting::value(
        name,
        SettingValue::Set {
            element: DataType::Text,
            packing: SetPacking::Delimited(",".to_string()),
        },
    )
    .expect("a set setting")
}
