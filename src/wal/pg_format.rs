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
pub use registry::{
    ColumnPlan, PgRelationFormat, PgTypeRegistry, PgTypeRegistryError, UnsendableColumn,
};

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

    /// A published column whose type travels in binary while a type nested in
    /// it has no send function, so the server's send call for it raises.
    #[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
    #[error(
        "column {schema}.{table}.{column} cannot travel in binary: its type {type_oid} nests type \
         {unsendable_type_oid}, which has no binary send function"
    )]
    pub struct UnsendableColumn {
        schema: String,
        table: String,
        column: String,
        type_oid: u32,
        unsendable_type_oid: u32,
    }

    impl UnsendableColumn {
        /// The schema of the column's table.
        #[must_use]
        pub fn schema(&self) -> &str {
            &self.schema
        }

        /// The column's table.
        #[must_use]
        pub fn table(&self) -> &str {
            &self.table
        }

        /// The column.
        #[must_use]
        pub fn column(&self) -> &str {
            &self.column
        }

        /// The column's own type.
        #[must_use]
        pub const fn type_oid(&self) -> u32 {
            self.type_oid
        }

        /// The nested type without a send function.
        #[must_use]
        pub const fn unsendable_type_oid(&self) -> u32 {
            self.unsendable_type_oid
        }
    }

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

    #[derive(Clone, Debug, PartialEq, Eq)]
    struct TypeShape {
        sends_binary: bool,
        domain_base: Option<u32>,
        /// An array's element, a range's subtype or a composite's fields.
        nested: Vec<u32>,
    }

    /// Which types a binary pgoutput stream sends in binary, the type a
    /// domain's binary cell reads as, and the types each one nests.
    ///
    /// Hand-assigned OIDs are known from `pg_type.dat`. Every other type is
    /// added from `pg_type` when the stream starts, and a type created later
    /// stays unknown to that stream.
    #[derive(Clone, Debug, Default, PartialEq, Eq)]
    pub struct PgTypeRegistry {
        types: BTreeMap<u32, TypeShape>,
    }

    impl PgTypeRegistry {
        /// `FirstGenbkiObjectId`: OIDs below it are hand-assigned in
        /// `pg_type.dat` and stable across major versions.
        pub(crate) const FIRST_UNPINNED_OID: u32 = 10_000;

        /// The query whose rows [`Self::with_type_row`] reads, every type
        /// with its domain base, array element, range subtype and composite
        /// fields.
        #[cfg(feature = "pg-streaming")]
        pub(crate) const TYPES_SQL: &'static str = "SELECT t.oid, t.typsend <> 0, t.typbasetype, \
             CASE WHEN t.typsubscript = 'pg_catalog.array_subscript_handler'::regproc \
             THEN t.typelem ELSE 0 END, \
             COALESCE(r.rngsubtype, m.rngsubtype, 0), \
             COALESCE((SELECT string_agg(a.atttypid::text, ',' ORDER BY a.attnum) \
             FROM pg_catalog.pg_attribute a \
             WHERE a.attrelid = t.typrelid AND a.attnum > 0 AND NOT a.attisdropped), '') \
             FROM pg_catalog.pg_type t \
             LEFT JOIN pg_catalog.pg_range r ON r.rngtypid = t.oid \
             LEFT JOIN pg_catalog.pg_range m ON m.rngmultitypid = t.oid";

        /// A registry that knows only the hand-assigned types.
        #[must_use]
        pub fn builtin() -> Self {
            Self::default()
        }

        /// Record the type `oid`, whether it has a send function, the type it
        /// is a domain over, if it is one, and the types it nests: an array's
        /// element, a range's subtype or a composite's fields.
        #[must_use]
        pub fn with_type(
            mut self,
            oid: u32,
            sends_binary: bool,
            domain_base: Option<u32>,
            nested: impl IntoIterator<Item = u32>,
        ) -> Self {
            self.types.insert(
                oid,
                TypeShape {
                    sends_binary,
                    domain_base,
                    nested: nested.into_iter().collect(),
                },
            );
            self
        }

        /// Record one row of [`Self::TYPES_SQL`] in its text form.
        ///
        /// # Errors
        ///
        /// [`PgTypeRegistryError::MalformedColumn`] when a column is not the
        /// OID, boolean or OID list text its query column produces.
        #[cfg(feature = "pg-streaming")]
        pub(crate) fn with_type_row(
            self,
            oid: &str,
            sends_binary: &str,
            domain_base: &str,
            element: &str,
            range_subtype: &str,
            fields: &str,
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
            let mut nested = Vec::new();
            for (column, text) in [("typelem", element), ("rngsubtype", range_subtype)] {
                let oid = read_oid(column, text)?;
                if oid != 0 {
                    nested.push(oid);
                }
            }
            if !fields.is_empty() {
                for field in fields.split(',') {
                    nested.push(read_oid("fields", field)?);
                }
            }
            Ok(self.with_type(
                read_oid("oid", oid)?,
                sends_binary,
                (base != 0).then_some(base),
                nested,
            ))
        }

        /// Whether the server writes a value of type `oid` with a send function.
        #[cfg(feature = "pg-streaming")]
        fn sends_binary(&self, oid: u32) -> bool {
            self.types.get(&oid).map_or_else(
                || !BUILTIN_WITHOUT_SEND.contains(&oid),
                |shape| shape.sends_binary,
            )
        }

        /// A type nested in `oid` that has no send function, when `oid` itself
        /// has one and so travels in binary on a binary stream.
        ///
        /// `array_send`, `range_send` and `record_send` raise on such a type,
        /// so no binary stream can carry a column of type `oid`.
        #[cfg(feature = "pg-streaming")]
        pub(crate) fn unsendable_within(&self, oid: u32) -> Option<u32> {
            if !self.sends_binary(oid) {
                return None;
            }
            let mut seen = alloc::collections::BTreeSet::from([oid]);
            let mut pending = vec![oid];
            while let Some(current) = pending.pop() {
                let Some(shape) = self.types.get(&current) else {
                    continue;
                };
                for &inner in shape.domain_base.iter().chain(&shape.nested) {
                    if !seen.insert(inner) {
                        continue;
                    }
                    if !self.sends_binary(inner) {
                        return Some(inner);
                    }
                    pending.push(inner);
                }
            }
            None
        }

        /// `column` of `schema.table`, of type `type_oid`, when a type nested
        /// in it has no send function.
        #[cfg(feature = "pg-streaming")]
        pub(crate) fn unsendable_column(
            &self,
            schema: &str,
            table: &str,
            column: &str,
            type_oid: u32,
        ) -> Option<UnsendableColumn> {
            use alloc::string::ToString;

            self.unsendable_within(type_oid)
                .map(|unsendable_type_oid| UnsendableColumn {
                    schema: schema.to_string(),
                    table: table.to_string(),
                    column: column.to_string(),
                    type_oid,
                    unsendable_type_oid,
                })
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
            let Some(shape) = self.types.get(&oid) else {
                return ColumnPlan::Unresolved {
                    type_oid: Some(oid),
                };
            };
            if !shape.sends_binary {
                return ColumnPlan::Text;
            }
            let mut reader_oid = oid;
            for _ in 0..MAX_DOMAIN_DEPTH {
                match self
                    .types
                    .get(&reader_oid)
                    .and_then(|shape| shape.domain_base)
                {
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

        #[cfg(feature = "pg-streaming")]
        const ACLITEM: u32 = 1033;
        #[cfg(feature = "pg-streaming")]
        const ACLITEM_ARRAY: u32 = 1034;
        #[cfg(feature = "pg-streaming")]
        const INT4: u32 = 23;

        #[test]
        #[cfg(feature = "pg-streaming")]
        fn a_registry_row_reads_its_six_columns() {
            let registry = PgTypeRegistry::builtin()
                .with_type_row("16500", "t", "23", "0", "0", "")
                .and_then(|r| r.with_type_row("16600", "f", "0", "0", "0", ""))
                .and_then(|r| r.with_type_row("16700", "t", "0", "16600", "0", ""))
                .and_then(|r| r.with_type_row("16800", "t", "0", "0", "701", ""))
                .and_then(|r| r.with_type_row("16900", "t", "0", "0", "0", "23,16700"))
                .expect("rows read");
            assert_eq!(
                registry,
                PgTypeRegistry::builtin()
                    .with_type(16_500, true, Some(23), [])
                    .with_type(16_600, false, None, [])
                    .with_type(16_700, true, None, [16_600])
                    .with_type(16_800, true, None, [701])
                    .with_type(16_900, true, None, [23, 16_700])
            );
            assert_eq!(
                PgTypeRegistry::builtin().with_type_row("16500", "yes", "0", "0", "0", ""),
                Err(super::PgTypeRegistryError::MalformedColumn {
                    column: "typsend",
                    text: "yes".into()
                })
            );
            assert_eq!(
                PgTypeRegistry::builtin().with_type_row("16500", "t", "0", "0", "0", "23,x"),
                Err(super::PgTypeRegistryError::MalformedColumn {
                    column: "fields",
                    text: "x".into()
                })
            );
        }

        #[test]
        fn a_domain_chain_reads_as_the_type_it_ends_at() {
            let registry = PgTypeRegistry::builtin()
                .with_type(16_500, true, Some(16_501), [])
                .with_type(16_501, true, Some(20), []);
            assert_eq!(registry.plan(16_500), ColumnPlan::Binary { reader_oid: 20 });
        }

        /// A type without a send function travels as text, so it alone is
        /// no obstacle to a binary stream.
        #[test]
        #[cfg(feature = "pg-streaming")]
        fn a_type_without_send_holds_nothing_unsendable() {
            let registry = PgTypeRegistry::builtin().with_type(ACLITEM, false, None, []);
            assert_eq!(registry.unsendable_within(ACLITEM), None);
            assert_eq!(registry.unsendable_within(INT4), None);
        }

        /// `array_send` needs its element's send function, so an array of a
        /// type without one cannot travel in binary.
        #[test]
        #[cfg(feature = "pg-streaming")]
        fn an_array_of_a_type_without_send_is_unsendable() {
            let registry = PgTypeRegistry::builtin()
                .with_type(ACLITEM, false, None, [])
                .with_type(ACLITEM_ARRAY, true, None, [ACLITEM]);
            assert_eq!(registry.unsendable_within(ACLITEM_ARRAY), Some(ACLITEM));
        }

        /// A hand-assigned element the catalog did not list is judged from
        /// `pg_type.dat`.
        #[test]
        #[cfg(feature = "pg-streaming")]
        fn a_hand_assigned_element_is_judged_without_its_row() {
            let registry =
                PgTypeRegistry::builtin().with_type(ACLITEM_ARRAY, true, None, [ACLITEM]);
            assert_eq!(registry.unsendable_within(ACLITEM_ARRAY), Some(ACLITEM));
        }

        /// Ranges, composite fields and domain bases nest the same way.
        #[test]
        #[cfg(feature = "pg-streaming")]
        fn every_nesting_reaches_the_type_without_send() {
            let range = 16_500;
            let composite = 16_600;
            let domain = 16_700;
            let registry = PgTypeRegistry::builtin()
                .with_type(ACLITEM, false, None, [])
                .with_type(ACLITEM_ARRAY, true, None, [ACLITEM])
                .with_type(range, true, None, [ACLITEM])
                .with_type(composite, true, None, [INT4, ACLITEM_ARRAY])
                .with_type(domain, true, Some(ACLITEM_ARRAY), []);
            for oid in [range, composite, domain] {
                assert_eq!(registry.unsendable_within(oid), Some(ACLITEM), "type {oid}");
            }
            let sendable = PgTypeRegistry::builtin().with_type(composite, true, None, [INT4, 25]);
            assert_eq!(sendable.unsendable_within(composite), None);
        }

        #[test]
        #[cfg(feature = "pg-streaming")]
        fn a_nesting_cycle_ends() {
            let registry = PgTypeRegistry::builtin()
                .with_type(16_500, true, None, [16_501])
                .with_type(16_501, true, None, [16_500]);
            assert_eq!(registry.unsendable_within(16_500), None);
        }

        #[test]
        fn a_domain_cycle_is_unresolved() {
            let registry = PgTypeRegistry::builtin()
                .with_type(16_500, true, Some(16_501), [])
                .with_type(16_501, true, Some(16_500), []);
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
