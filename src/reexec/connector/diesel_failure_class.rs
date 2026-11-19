//! [`DieselFailureClass`] and its impls for diesel's PostgreSQL, MySQL and
//! SQLite backends.

use super::FailureClass;
use diesel::result::{DatabaseErrorInformation, DatabaseErrorKind, Error};

/// Classifies a failed diesel read by the codes of the database the
/// connection talks to.
///
/// diesel's backend types implement it, because the codes come from the
/// connection, so a `SqliteConnection` read through
/// [`crate::backend::Postgres`] reports SQLite result codes.
pub trait DieselFailureClass: diesel::backend::Backend {
    /// Classify a database error by the code the server or the driver
    /// reported.
    #[must_use]
    fn database_failure_class(information: &dyn DatabaseErrorInformation) -> FailureClass;

    /// Transient for a lost connection and a transaction-manager error,
    /// permanent for an error raised before the query reaches the database,
    /// and [`database_failure_class`](Self::database_failure_class) for any
    /// other database error.
    #[must_use]
    fn failure_class(error: &Error) -> FailureClass {
        match error {
            Error::DatabaseError(
                DatabaseErrorKind::ClosedConnection | DatabaseErrorKind::UnableToSendCommand,
                _,
            ) => FailureClass::Transient,
            Error::DatabaseError(_, information) => {
                Self::database_failure_class(information.as_ref())
            }
            Error::QueryBuilderError(_)
            | Error::SerializationError(_)
            | Error::DeserializationError(_)
            | Error::InvalidCString(_)
            | Error::IntegerConversion(_)
            | Error::NotFound => FailureClass::Permanent,
            _ => FailureClass::Transient,
        }
    }
}

#[cfg(any(
    feature = "diesel-typed",
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-async-postgres",
    feature = "pgoutput-binary"
))]
impl DieselFailureClass for diesel::pg::Pg {
    fn database_failure_class(information: &dyn DatabaseErrorInformation) -> FailureClass {
        postgres_sqlstate_failure_class(
            information
                .sqlstate()
                .as_ref()
                .map(diesel::result::SqlState::as_str),
        )
    }
}

/// Transient for an SQLSTATE on Npgsql's `PostgresException.IsTransient`
/// list and for an error without SQLSTATE, permanent for any other code.
#[cfg(any(
    feature = "diesel-typed",
    feature = "executor-diesel-postgres",
    feature = "executor-diesel-async-postgres",
    feature = "pgoutput-binary"
))]
fn postgres_sqlstate_failure_class(sqlstate: Option<&str>) -> FailureClass {
    match sqlstate {
        None
        | Some(
            "08000" | "08001" | "08003" | "08004" | "08006" | "08007" | "40001" | "40P01" | "53000"
            | "53100" | "53200" | "53300" | "53400" | "55000" | "55006" | "55P03" | "57P01"
            | "57P02" | "57P03" | "57P05" | "58000" | "58030",
        ) => FailureClass::Transient,
        Some(_) => FailureClass::Permanent,
    }
}

#[cfg(any(
    feature = "diesel-typed-mysql",
    feature = "executor-diesel-mysql",
    feature = "executor-diesel-async-mysql"
))]
impl DieselFailureClass for diesel::mysql::Mysql {
    fn database_failure_class(information: &dyn DatabaseErrorInformation) -> FailureClass {
        mysql_error_number_failure_class(information.native_code())
    }
}

/// Transient for an error number on MySqlConnector's
/// `MySqlException.IsTransient` list, for a client-side connection loss and
/// for an error without a number, permanent for any other number.
#[cfg(any(
    feature = "diesel-typed-mysql",
    feature = "executor-diesel-mysql",
    feature = "executor-diesel-async-mysql"
))]
const fn mysql_error_number_failure_class(number: Option<i32>) -> FailureClass {
    match number {
        None
        | Some(
            // Too many connections, bad handshake, lock wait timeout,
            // deadlock and a deadlock in an XA transaction.
            1040 | 1042 | 1205 | 1213 | 1614
            // Cannot connect by socket or TCP, server gone away, lost
            // connection during a query and lost connection after it.
            | 2002 | 2003 | 2006 | 2013 | 2055,
        ) => FailureClass::Transient,
        Some(_) => FailureClass::Permanent,
    }
}

#[cfg(any(feature = "diesel-typed-sqlite", feature = "pg-sqlite-emu"))]
impl DieselFailureClass for diesel::sqlite::Sqlite {
    fn database_failure_class(information: &dyn DatabaseErrorInformation) -> FailureClass {
        sqlite_result_code_failure_class(information.native_code())
    }
}

/// Transient for `SQLITE_BUSY` and `SQLITE_LOCKED` with any extended code,
/// permanent for any other result code and for an error diesel raises
/// without one.
#[cfg(any(feature = "diesel-typed-sqlite", feature = "pg-sqlite-emu"))]
fn sqlite_result_code_failure_class(code: Option<i32>) -> FailureClass {
    const SQLITE_BUSY: i32 = 5;
    const SQLITE_LOCKED: i32 = 6;

    // The low byte of an extended result code is its primary code.
    match code.map(|code| code & 0xff) {
        Some(SQLITE_BUSY | SQLITE_LOCKED) => FailureClass::Transient,
        _ => FailureClass::Permanent,
    }
}

#[cfg(all(
    test,
    any(
        feature = "diesel-typed",
        feature = "executor-diesel-postgres",
        feature = "executor-diesel-async-postgres",
        feature = "pgoutput-binary",
        feature = "diesel-typed-mysql",
        feature = "executor-diesel-mysql",
        feature = "executor-diesel-async-mysql",
        feature = "diesel-typed-sqlite",
        feature = "pg-sqlite-emu"
    )
))]
mod tests {
    use alloc::boxed::Box;
    use alloc::string::String;

    use super::{DatabaseErrorKind, DieselFailureClass, Error};
    use crate::reexec::FailureClass;

    fn assert_classes<B: DieselFailureClass>(
        cases: impl IntoIterator<Item = Error>,
        expected: FailureClass,
    ) {
        for error in cases {
            assert_eq!(B::failure_class(&error), expected, "{error:?}");
        }
    }

    /// A database error the driver raised with a message and no code.
    fn uncoded(kind: DatabaseErrorKind) -> Error {
        Error::DatabaseError(kind, Box::new(String::from("driver")))
    }

    /// Errors whose class every backend shares.
    fn assert_shared_classes<B: DieselFailureClass>() {
        assert_classes::<B>(
            [
                Error::BrokenTransactionManager,
                Error::RollbackTransaction,
                Error::AlreadyInTransaction,
                Error::NotInTransaction,
                Error::ClosingHandle("handle"),
                Error::RollbackErrorOnCommit {
                    rollback_error: Box::new(Error::NotFound),
                    commit_error: Box::new(Error::NotFound),
                },
                uncoded(DatabaseErrorKind::ClosedConnection),
                uncoded(DatabaseErrorKind::UnableToSendCommand),
            ],
            FailureClass::Transient,
        );
        assert_classes::<B>(
            [
                Error::NotFound,
                Error::QueryBuilderError("query".into()),
                Error::SerializationError("serialization".into()),
                Error::DeserializationError("deserialization".into()),
                Error::InvalidCString(
                    alloc::ffi::CString::new(b"embedded\0nul".to_vec()).expect_err("embedded nul"),
                ),
                Error::IntegerConversion(u8::try_from(256_u16).expect_err("outside byte range")),
            ],
            FailureClass::Permanent,
        );
    }

    #[cfg(any(
        feature = "diesel-typed",
        feature = "executor-diesel-postgres",
        feature = "executor-diesel-async-postgres",
        feature = "pgoutput-binary"
    ))]
    #[test]
    fn postgres_classifies_by_the_npgsql_sqlstate_list() {
        use super::postgres_sqlstate_failure_class;

        assert_shared_classes::<diesel::pg::Pg>();
        assert_classes::<diesel::pg::Pg>(
            [uncoded(DatabaseErrorKind::Unknown)],
            FailureClass::Transient,
        );
        for (sqlstates, expected) in [
            (
                &[
                    "08000", "08001", "08003", "08004", "08006", "08007", "40001", "40P01",
                    "53000", "53100", "53200", "53300", "53400", "55000", "55006", "55P03",
                    "57P01", "57P02", "57P03", "57P05", "58000", "58030",
                ][..],
                FailureClass::Transient,
            ),
            (
                &[
                    "42P01", "42703", "42501", "57014", "57P04", "23505", "08002", "58001",
                ][..],
                FailureClass::Permanent,
            ),
        ] {
            for sqlstate in sqlstates {
                assert_eq!(
                    postgres_sqlstate_failure_class(Some(sqlstate)),
                    expected,
                    "{sqlstate}"
                );
            }
        }
    }

    #[cfg(any(
        feature = "diesel-typed-mysql",
        feature = "executor-diesel-mysql",
        feature = "executor-diesel-async-mysql"
    ))]
    #[test]
    fn mysql_classifies_by_the_error_number() {
        use super::mysql_error_number_failure_class;

        assert_shared_classes::<diesel::mysql::Mysql>();
        assert_classes::<diesel::mysql::Mysql>(
            [uncoded(DatabaseErrorKind::Unknown)],
            FailureClass::Transient,
        );
        for number in [1040, 1042, 1205, 1213, 1614, 2002, 2003, 2006, 2013, 2055] {
            assert_eq!(
                mysql_error_number_failure_class(Some(number)),
                FailureClass::Transient,
                "{number}"
            );
        }
        // Unknown table, unknown column, denied command, syntax error and
        // duplicate key.
        for number in [1146, 1054, 1142, 1064, 1062] {
            assert_eq!(
                mysql_error_number_failure_class(Some(number)),
                FailureClass::Permanent,
                "{number}"
            );
        }
    }

    #[cfg(any(feature = "diesel-typed-sqlite", feature = "pg-sqlite-emu"))]
    #[test]
    fn sqlite_classifies_by_the_primary_result_code() {
        use super::sqlite_result_code_failure_class;

        assert_shared_classes::<diesel::sqlite::Sqlite>();
        assert_classes::<diesel::sqlite::Sqlite>(
            [uncoded(DatabaseErrorKind::Unknown)],
            FailureClass::Permanent,
        );
        // `SQLITE_BUSY`, `SQLITE_LOCKED`, `SQLITE_BUSY_RECOVERY`,
        // `SQLITE_BUSY_SNAPSHOT` and `SQLITE_LOCKED_SHAREDCACHE`.
        for code in [5, 6, 261, 517, 262] {
            assert_eq!(
                sqlite_result_code_failure_class(Some(code)),
                FailureClass::Transient,
                "{code}"
            );
        }
        // `SQLITE_ERROR`, `SQLITE_READONLY`, `SQLITE_CORRUPT`, `SQLITE_AUTH`
        // and `SQLITE_CONSTRAINT_UNIQUE`.
        for code in [1, 8, 11, 23, 2067] {
            assert_eq!(
                sqlite_result_code_failure_class(Some(code)),
                FailureClass::Permanent,
                "{code}"
            );
        }
    }
}
