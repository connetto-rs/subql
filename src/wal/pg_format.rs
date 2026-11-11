//! The format each pgoutput column travels in, and the type a binary column
//! reads as.
//!
//! On a stream that asked for `binary on`, PostgreSQL sends a column in binary
//! exactly when its type has a send function, and as text otherwise
//! (`logicalrep_write_tuple` in `proto.c`). The stream never says which types
//! have one, so a [`PgTypeRegistry`] read when the stream starts answers it,
//! and every column's format is fixed from its type OID when its relation is
//! announced.

/// The format of one pgoutput cell, as its format byte names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PgCellFormat {
    /// The type's output function wrote it.
    Text,
    /// The type's send function wrote it.
    Binary,
}

#[cfg(any(feature = "pg-streaming", feature = "pg-sqlite-emu"))]
pub use registry::{ColumnPlan, PgRelationFormat, PgTypeRegistry, PgTypeRegistryError};

#[cfg(any(feature = "pg-streaming", feature = "pg-sqlite-emu"))]
mod registry {
    use alloc::collections::BTreeMap;
    use alloc::string::String;
    use alloc::sync::Arc;
    use alloc::vec::Vec;

    use pg_walstream::RelationColumn;

    /// The hand-assigned types `pg_type.dat` declares without a send function,
    /// identical from PostgreSQL 14 through 18: `aclitem`, `gtsvector` and the
    /// pseudo-types.
    const BUILTIN_WITHOUT_SEND: [u32; 20] = [
        269, 325, 1033, 2276, 2279, 2280, 2281, 2283, 2776, 3115, 3310, 3500, 3642, 3831, 3838,
        4537, 4538, 5077, 5079, 5080,
    ];

    /// A domain chain longer than this is a cycle in a corrupt registry.
    const MAX_DOMAIN_DEPTH: usize = 64;

    /// A `pg_type` row the registry query returned in a shape it cannot read.
    #[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
    pub enum PgTypeRegistryError {
        /// A column held text that is not the value its query column produces.
        #[error("pg_type row column `{column}` holds `{text}`")]
        MalformedColumn {
            /// The query column that could not be read.
            column: &'static str,
            /// The text it held.
            text: String,
        },
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct UserType {
        sends_binary: bool,
        domain_base: Option<u32>,
    }

    /// Which types a binary pgoutput stream sends in binary, and the type a
    /// domain's binary cell reads as.
    ///
    /// Hand-assigned OIDs are known from `pg_type.dat`. Every other type is
    /// added from `pg_type` when the stream starts, and a type created later
    /// stays unknown to that stream.
    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    pub struct PgTypeRegistry {
        user: BTreeMap<u32, UserType>,
    }

    impl PgTypeRegistry {
        /// `FirstGenbkiObjectId`: OIDs below it are hand-assigned in
        /// `pg_type.dat` and stable across major versions.
        pub(crate) const FIRST_UNPINNED_OID: u32 = 10_000;

        /// The query whose rows [`Self::with_user_type_row`] reads.
        #[cfg(feature = "pg-streaming")]
        pub(crate) const USER_TYPES_SQL: &'static str = "SELECT t.oid, t.typsend <> 0, t.typbasetype FROM pg_catalog.pg_type t WHERE t.oid >= 10000";

        /// A registry that knows only the hand-assigned types.
        #[must_use]
        pub fn builtin() -> Self {
            Self::default()
        }

        /// Record the type `oid`, whether it has a send function, and the
        /// type it is a domain over, if it is one.
        #[must_use]
        pub fn with_user_type(
            mut self,
            oid: u32,
            sends_binary: bool,
            domain_base: Option<u32>,
        ) -> Self {
            self.user.insert(
                oid,
                UserType {
                    sends_binary,
                    domain_base,
                },
            );
            self
        }

        /// Record one row of [`Self::USER_TYPES_SQL`] in its text form.
        ///
        /// # Errors
        ///
        /// [`PgTypeRegistryError::MalformedColumn`] when a column is not the
        /// OID or boolean text its query column produces.
        #[cfg(feature = "pg-streaming")]
        pub(crate) fn with_user_type_row(
            self,
            oid: &str,
            sends_binary: &str,
            domain_base: &str,
        ) -> Result<Self, PgTypeRegistryError> {
            use alloc::string::ToString;

            let read_oid = |column, text: &str| {
                text.parse::<u32>()
                    .map_err(|_| PgTypeRegistryError::MalformedColumn {
                        column,
                        text: text.to_string(),
                    })
            };
            let sends_binary = match sends_binary {
                "t" => true,
                "f" => false,
                other => {
                    return Err(PgTypeRegistryError::MalformedColumn {
                        column: "typsend",
                        text: other.to_string(),
                    })
                }
            };
            let base = read_oid("typbasetype", domain_base)?;
            Ok(self.with_user_type(
                read_oid("oid", oid)?,
                sends_binary,
                (base != 0).then_some(base),
            ))
        }

        /// How a binary stream sends and reads a column of type `oid`.
        fn plan(&self, oid: u32) -> ColumnPlan {
            if oid < Self::FIRST_UNPINNED_OID {
                return if BUILTIN_WITHOUT_SEND.contains(&oid) {
                    ColumnPlan::Text
                } else {
                    ColumnPlan::Binary { reader_oid: oid }
                };
            }
            let Some(user) = self.user.get(&oid) else {
                return ColumnPlan::Unresolved {
                    type_oid: Some(oid),
                };
            };
            if !user.sends_binary {
                return ColumnPlan::Text;
            }
            let mut reader_oid = oid;
            for _ in 0..MAX_DOMAIN_DEPTH {
                match self.user.get(&reader_oid).and_then(|user| user.domain_base) {
                    Some(base) => reader_oid = base,
                    None => return ColumnPlan::Binary { reader_oid },
                }
            }
            ColumnPlan::Unresolved {
                type_oid: Some(oid),
            }
        }
    }

    /// How one column of a stream is sent and read.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum ColumnPlan {
        /// Every cell is text.
        Text,
        /// Every cell is binary, read as the type `reader_oid`.
        Binary {
            /// The column's type, or the type a domain chain ends at.
            reader_oid: u32,
        },
        /// The format is not known, because the registry does not know the
        /// type or the relation was never announced.
        Unresolved {
            /// The column's type, when the relation named it.
            type_oid: Option<u32>,
        },
    }

    /// The plan of every column of one relation on a binary stream.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct PgRelationFormat {
        columns: Vec<(Arc<str>, ColumnPlan)>,
    }

    impl PgRelationFormat {
        /// Plan `columns` as `registry` says their types are sent.
        pub fn plan(registry: &PgTypeRegistry, columns: &[RelationColumn]) -> Self {
            Self {
                columns: columns
                    .iter()
                    .map(|column| (Arc::clone(&column.name), registry.plan(column.type_id)))
                    .collect(),
            }
        }

        /// The format of a relation the stream never announced.
        pub const fn unannounced() -> Self {
            Self {
                columns: Vec::new(),
            }
        }

        /// The plan of the column `name`.
        pub fn column(&self, name: &str) -> ColumnPlan {
            self.columns
                .iter()
                .find(|(column, _)| &**column == name)
                .map_or(ColumnPlan::Unresolved { type_oid: None }, |&(_, plan)| plan)
        }
    }

    #[cfg(test)]
    mod tests {
        use super::{ColumnPlan, PgTypeRegistry};

        #[test]
        #[cfg(feature = "pg-streaming")]
        fn a_registry_row_reads_its_three_columns() {
            let registry = PgTypeRegistry::builtin()
                .with_user_type_row("16500", "t", "23")
                .and_then(|r| r.with_user_type_row("16600", "f", "0"))
                .expect("rows read");
            assert_eq!(
                registry,
                PgTypeRegistry::builtin()
                    .with_user_type(16_500, true, Some(23))
                    .with_user_type(16_600, false, None)
            );
            assert_eq!(
                PgTypeRegistry::builtin().with_user_type_row("16500", "yes", "0"),
                Err(super::PgTypeRegistryError::MalformedColumn {
                    column: "typsend",
                    text: "yes".into()
                })
            );
        }

        #[test]
        fn a_domain_chain_reads_as_the_type_it_ends_at() {
            let registry = PgTypeRegistry::builtin()
                .with_user_type(16_500, true, Some(16_501))
                .with_user_type(16_501, true, Some(20));
            assert_eq!(registry.plan(16_500), ColumnPlan::Binary { reader_oid: 20 });
        }

        #[test]
        fn a_domain_cycle_is_unresolved() {
            let registry = PgTypeRegistry::builtin()
                .with_user_type(16_500, true, Some(16_501))
                .with_user_type(16_501, true, Some(16_500));
            assert_eq!(
                registry.plan(16_500),
                ColumnPlan::Unresolved {
                    type_oid: Some(16_500)
                }
            );
        }

        #[test]
        fn an_unpinned_type_below_user_oids_needs_the_registry() {
            // information_schema domains take OIDs from 10000 at initdb.
            assert_eq!(
                PgTypeRegistry::builtin().plan(13_000),
                ColumnPlan::Unresolved {
                    type_oid: Some(13_000)
                }
            );
            assert_eq!(
                PgTypeRegistry::builtin().plan(9_999),
                ColumnPlan::Binary { reader_oid: 9_999 }
            );
        }
    }
}
