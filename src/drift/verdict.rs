//! The drift judgment: a catalog shape against a live shape.
//!
//! [`judge`] compares the consumer's fixed catalog shape of one table
//! against the live shape, or a missing live table, and answers the
//! verdict the vocabulary fixes: unchanged, allowed additions,
//! incompatible with every reason, or identity weakened. A renamed
//! column reads as a drop plus an add, since the judgment compares
//! names and a rename is exactly that on the wire.

use alloc::string::String;
use alloc::vec::Vec;

use super::shape::{ReplicaIdentity, TableShape};

/// One reason a live table no longer conforms to the catalog's.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Incompatibility {
    /// The table is missing from the live database.
    TableMissing,
    /// A catalog column is absent from the live shape.
    ColumnDropped {
        /// The catalog column's name.
        column: String,
    },
    /// A live column that is neither in the catalog nor nullable nor
    /// defaulted, so a write the catalog allows the live table rejects.
    ColumnNotAddedNullSafe {
        /// The live column's name.
        column: String,
    },
    /// A column present in both declares a different type or bounds.
    ColumnRetyped {
        /// The column's name.
        column: String,
        /// The catalog's normalized type spelling.
        catalog: String,
        /// The live normalized type spelling.
        live: String,
    },
    /// A column present in both declares a different nullability.
    ColumnNullabilityChanged {
        /// The column's name.
        column: String,
        /// The catalog's nullability.
        catalog_nullable: bool,
        /// The live nullability.
        live_nullable: bool,
    },
    /// A `NOT NULL` column present in both lost the default the catalog
    /// declares, so a write that omits it, which the catalog allows, fails.
    ColumnDefaultDropped {
        /// The column's name.
        column: String,
    },
    /// A live `CHECK` constraint with no counterpart in the catalog.
    CheckAdded {
        /// The live expression.
        expression: String,
    },
    /// A `CHECK` present in both carries a different expression.
    CheckChanged {
        /// The catalog expression.
        catalog: String,
        /// The live expression.
        live: String,
    },
    /// A catalog `CHECK` constraint absent from the live shape.
    CheckDropped {
        /// The catalog expression.
        expression: String,
    },
    /// The primary key's columns or their order differ.
    PrimaryKeyChanged {
        /// The catalog's key columns, in key order.
        catalog: Vec<String>,
        /// The live key columns, in key order.
        live: Vec<String>,
    },
    /// The live replica identity is other than `FULL`.
    IdentityWeakened {
        /// The identity the live backend states.
        identity: ReplicaIdentity,
    },
}

/// The judgment of a live shape against the catalog's table.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Verdict {
    /// The live shape is the catalog's, nothing to do.
    Unchanged,
    /// Every catalog column is present with the same type and
    /// nullability and every extra column is nullable or has a default.
    /// The extra columns are projected away by the reader.
    AllowedAdditions,
    /// The live table no longer conforms, with every reason.
    Incompatible {
        /// Every reason, in a fixed order.
        reasons: Vec<Incompatibility>,
    },
    /// The live replica identity is other than `FULL`.
    IdentityWeakened(ReplicaIdentity),
}

/// Judge the live shape, or a missing live table, against the catalog's.
///
/// The identity is judged before the shape: a live identity other than
/// `FULL` refuses the table on its own, since the old-image reasoning a
/// consumer builds on needs every column. The shape judgment runs when
/// the identity holds, and its reasons come back in a fixed order,
/// catalog columns first in catalog order, then the live extras in live
/// order, the key, and the `CHECK` constraints.
#[must_use]
pub fn judge(catalog: &TableShape, live: Option<&TableShape>) -> Verdict {
    let Some(live) = live else {
        return Verdict::Incompatible {
            reasons: alloc::vec![Incompatibility::TableMissing],
        };
    };
    if let Some(identity) = live.replica_identity() {
        if identity != ReplicaIdentity::Full {
            return Verdict::IdentityWeakened(identity);
        }
    }
    let mut reasons = Vec::new();
    for catalog_column in catalog.columns() {
        match live
            .columns()
            .iter()
            .find(|c| c.name() == catalog_column.name())
        {
            None => reasons.push(Incompatibility::ColumnDropped {
                column: catalog_column.name().into(),
            }),
            Some(live_column) => {
                if !live_column.type_matches(catalog_column) {
                    reasons.push(Incompatibility::ColumnRetyped {
                        column: catalog_column.name().into(),
                        catalog: catalog_column.type_normalized().into(),
                        live: live_column.type_normalized().into(),
                    });
                }
                if live_column.nullable() != catalog_column.nullable() {
                    reasons.push(Incompatibility::ColumnNullabilityChanged {
                        column: catalog_column.name().into(),
                        catalog_nullable: catalog_column.nullable(),
                        live_nullable: live_column.nullable(),
                    });
                }
                if catalog_column.has_default()
                    && !live_column.has_default()
                    && !live_column.nullable()
                {
                    reasons.push(Incompatibility::ColumnDefaultDropped {
                        column: catalog_column.name().into(),
                    });
                }
            }
        }
    }
    for live_column in live.columns() {
        if catalog
            .columns()
            .iter()
            .any(|c| c.name() == live_column.name())
        {
            continue;
        }
        if !live_column.nullable() && !live_column.has_default() {
            reasons.push(Incompatibility::ColumnNotAddedNullSafe {
                column: live_column.name().into(),
            });
        }
    }
    if live.primary_key() != catalog.primary_key() {
        reasons.push(Incompatibility::PrimaryKeyChanged {
            catalog: catalog.primary_key().to_vec(),
            live: live.primary_key().to_vec(),
        });
    }
    check_reasons(catalog, live, &mut reasons);
    if reasons.is_empty() {
        if live
            .columns()
            .iter()
            .zip(catalog.columns().iter())
            .all(|(live, catalog)| live.name() == catalog.name())
            && live.columns().len() == catalog.columns().len()
        {
            Verdict::Unchanged
        } else {
            Verdict::AllowedAdditions
        }
    } else {
        Verdict::Incompatible { reasons }
    }
}

/// Pair the catalog's and the live `CHECK` expressions and report every
/// difference. SQLite names no constraint, so the two sides compare as a
/// multiset of expressions, each occurrence matching one equal occurrence
/// on the other side: a reordered equal set costs no reason, the unmatched
/// ones pair by position as changes, and whatever is left over is a drop
/// or an add.
fn check_reasons(catalog: &TableShape, live: &TableShape, reasons: &mut Vec<Incompatibility>) {
    let catalog_checks = catalog.checks();
    let live_checks = live.checks();
    let catalog_unmatched = unmatched_indexes(catalog_checks, live_checks);
    let live_unmatched = unmatched_indexes(live_checks, catalog_checks);
    let matched = core::cmp::min(catalog_unmatched.len(), live_unmatched.len());
    for (i, j) in catalog_unmatched.iter().zip(live_unmatched.iter()) {
        reasons.push(Incompatibility::CheckChanged {
            catalog: catalog_checks[*i].clone(),
            live: live_checks[*j].clone(),
        });
    }
    for i in catalog_unmatched.iter().skip(matched) {
        reasons.push(Incompatibility::CheckDropped {
            expression: catalog_checks[*i].clone(),
        });
    }
    for j in live_unmatched.iter().skip(matched) {
        reasons.push(Incompatibility::CheckAdded {
            expression: live_checks[*j].clone(),
        });
    }
}

/// The indexes of `all` that hold an occurrence `other` lacks, in the
/// order `all` declares them. An occurrence at `i` is unmatched when the
/// occurrences of the same expression from the start of `all` through `i`
/// outnumber the copies `other` carries.
fn unmatched_indexes(all: &[String], other: &[String]) -> Vec<usize> {
    (0..all.len())
        .filter(|i| {
            let upto_i = all.iter().take(*i + 1).filter(|c| *c == &all[*i]).count();
            let in_other = other.iter().filter(|c| *c == &all[*i]).count();
            upto_i > in_other
        })
        .collect()
}

/// The refusal's reasons, or `None` when the verdict allows the table.
///
/// The guard keeps the names of refused tables and hands these reasons
/// to the consumer, so an allowed table answers `None`.
#[must_use]
pub fn refusal_reasons(
    catalog: &TableShape,
    live: Option<&TableShape>,
) -> Option<Vec<Incompatibility>> {
    match judge(catalog, live) {
        Verdict::Unchanged | Verdict::AllowedAdditions => None,
        Verdict::Incompatible { reasons } => Some(reasons),
        Verdict::IdentityWeakened(identity) => {
            Some(Vec::from([Incompatibility::IdentityWeakened { identity }]))
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use alloc::vec;
    use sql_traits::prelude::DatabaseLike;
    use sql_traits::structs::ParserDB;
    use sqlparser::dialect::{Dialect, PostgreSqlDialect, SQLiteDialect};

    fn parse<D: Dialect + core::default::Default>(ddl: &str) -> ParserDB {
        ParserDB::parse::<D>(ddl).expect("DDL parses")
    }

    fn shape_of<D: Dialect + core::default::Default>(ddl: &str) -> TableShape {
        let db = parse::<D>(ddl);
        let table = db.tables().next().expect("one table");
        TableShape::from_table(table, &db).expect("shape builds")
    }

    fn sqlite(catalog: &str, live: &str) -> (TableShape, Option<TableShape>) {
        (
            shape_of::<SQLiteDialect>(catalog),
            Some(shape_of::<SQLiteDialect>(live)),
        )
    }

    fn postgres(catalog: &str, live: &str) -> (TableShape, Option<TableShape>) {
        (
            shape_of::<PostgreSqlDialect>(catalog),
            Some(shape_of::<PostgreSqlDialect>(live)),
        )
    }

    const BASE: &str = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER, note TEXT)";

    #[test]
    fn an_unchanged_table_is_unchanged() {
        let (catalog, live) = sqlite(BASE, BASE);
        assert_eq!(judge(&catalog, live.as_ref()), Verdict::Unchanged);
    }

    #[test]
    fn an_unchanged_check_keeps_its_verdict() {
        let ddl = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER CHECK (v > 0))";
        let (catalog, live) = sqlite(ddl, ddl);
        assert_eq!(judge(&catalog, live.as_ref()), Verdict::Unchanged);
    }

    #[test]
    fn an_added_nullable_column_is_an_allowed_addition() {
        let live = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER, note TEXT, extra TEXT)";
        let (catalog, live) = sqlite(BASE, live);
        assert_eq!(judge(&catalog, live.as_ref()), Verdict::AllowedAdditions);
    }

    #[test]
    fn an_added_not_null_column_with_a_default_is_allowed() {
        let live =
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER, note TEXT, extra INTEGER NOT NULL DEFAULT 1)";
        let (catalog, live) = sqlite(BASE, live);
        assert_eq!(judge(&catalog, live.as_ref()), Verdict::AllowedAdditions);
    }

    #[test]
    fn an_added_not_null_column_without_a_default_is_incompatible() {
        let live =
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER, note TEXT, extra INTEGER NOT NULL)";
        let (catalog, live) = sqlite(BASE, live);
        assert_eq!(
            judge(&catalog, live.as_ref()),
            Verdict::Incompatible {
                reasons: vec![Incompatibility::ColumnNotAddedNullSafe {
                    column: "extra".into()
                }]
            }
        );
    }

    #[test]
    fn a_dropped_column_is_incompatible() {
        let live = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER)";
        let (catalog, live) = sqlite(BASE, live);
        assert_eq!(
            judge(&catalog, live.as_ref()),
            Verdict::Incompatible {
                reasons: vec![Incompatibility::ColumnDropped {
                    column: "note".into()
                }]
            }
        );
    }

    #[test]
    fn a_renamed_column_is_a_drop_plus_an_add() {
        let live = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER, renamed TEXT NOT NULL)";
        let (catalog, live) = sqlite(BASE, live);
        assert_eq!(
            judge(&catalog, live.as_ref()),
            Verdict::Incompatible {
                reasons: vec![
                    Incompatibility::ColumnDropped {
                        column: "note".into()
                    },
                    Incompatibility::ColumnNotAddedNullSafe {
                        column: "renamed".into()
                    },
                ]
            }
        );
    }

    #[test]
    fn a_retyped_column_is_incompatible_in_either_direction() {
        let live = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v TEXT, note TEXT)";
        let (catalog, live) = sqlite(BASE, live);
        assert_eq!(
            judge(&catalog, live.as_ref()),
            Verdict::Incompatible {
                reasons: vec![Incompatibility::ColumnRetyped {
                    column: "v".into(),
                    catalog: "INT".into(),
                    live: "TEXT".into(),
                }]
            }
        );
    }

    #[test]
    fn a_changed_bound_is_incompatible_in_either_direction() {
        let widened = postgres(
            "CREATE TABLE t1 (v VARCHAR(10))",
            "CREATE TABLE t1 (v VARCHAR(20))",
        );
        assert_eq!(
            judge(&widened.0, widened.1.as_ref()),
            Verdict::Incompatible {
                reasons: vec![Incompatibility::ColumnRetyped {
                    column: "v".into(),
                    catalog: "VARCHAR".into(),
                    live: "VARCHAR".into(),
                }]
            }
        );
        let narrowed = postgres(
            "CREATE TABLE t1 (v VARCHAR(20))",
            "CREATE TABLE t1 (v VARCHAR(10))",
        );
        assert_eq!(
            judge(&narrowed.0, narrowed.1.as_ref()),
            Verdict::Incompatible {
                reasons: vec![Incompatibility::ColumnRetyped {
                    column: "v".into(),
                    catalog: "VARCHAR".into(),
                    live: "VARCHAR".into(),
                }]
            }
        );
    }

    #[test]
    fn an_integer_width_change_is_incompatible() {
        let (catalog, live) = postgres("CREATE TABLE t1 (v INT)", "CREATE TABLE t1 (v BIGINT)");
        assert_eq!(
            judge(&catalog, live.as_ref()),
            Verdict::Incompatible {
                reasons: vec![Incompatibility::ColumnRetyped {
                    column: "v".into(),
                    catalog: "INT".into(),
                    live: "BIGINT".into(),
                }]
            }
        );
    }

    #[test]
    fn a_numeric_scale_change_is_incompatible() {
        let (catalog, live) = postgres(
            "CREATE TABLE t1 (v NUMERIC(10, 2))",
            "CREATE TABLE t1 (v NUMERIC(12, 2))",
        );
        assert_eq!(
            judge(&catalog, live.as_ref()),
            Verdict::Incompatible {
                reasons: vec![Incompatibility::ColumnRetyped {
                    column: "v".into(),
                    catalog: "NUMERIC".into(),
                    live: "NUMERIC".into(),
                }]
            }
        );
    }

    #[test]
    fn a_nullability_change_is_incompatible_in_either_direction() {
        let (catalog, live) = (
            shape_of::<PostgreSqlDialect>("CREATE TABLE t1 (note TEXT NOT NULL)"),
            shape_of::<PostgreSqlDialect>("CREATE TABLE t1 (note TEXT)"),
        );
        assert_eq!(
            judge(&catalog, Some(&live)),
            Verdict::Incompatible {
                reasons: vec![Incompatibility::ColumnNullabilityChanged {
                    column: "note".into(),
                    catalog_nullable: false,
                    live_nullable: true,
                }]
            }
        );
        let (catalog, live) = (
            shape_of::<PostgreSqlDialect>("CREATE TABLE t1 (note TEXT)"),
            shape_of::<PostgreSqlDialect>("CREATE TABLE t1 (note TEXT NOT NULL)"),
        );
        assert_eq!(
            judge(&catalog, Some(&live)),
            Verdict::Incompatible {
                reasons: vec![Incompatibility::ColumnNullabilityChanged {
                    column: "note".into(),
                    catalog_nullable: true,
                    live_nullable: false,
                }]
            }
        );
    }

    #[test]
    fn a_default_dropped_from_a_not_null_column_is_incompatible() {
        let (catalog, live) = sqlite(
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER NOT NULL DEFAULT 0)",
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER NOT NULL)",
        );
        assert_eq!(
            judge(&catalog, live.as_ref()),
            Verdict::Incompatible {
                reasons: vec![Incompatibility::ColumnDefaultDropped { column: "v".into() }]
            }
        );
    }

    #[test]
    fn a_default_dropped_from_a_nullable_column_keeps_the_table() {
        let (catalog, live) = sqlite(
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER DEFAULT 0)",
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER)",
        );
        assert_eq!(judge(&catalog, live.as_ref()), Verdict::Unchanged);
    }

    #[test]
    fn an_added_check_is_incompatible() {
        let live = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER CHECK (v > 0), note TEXT)";
        let (catalog, live) = sqlite(BASE, live);
        assert_eq!(
            judge(&catalog, live.as_ref()),
            Verdict::Incompatible {
                reasons: vec![Incompatibility::CheckAdded {
                    expression: "v > 0".into(),
                }]
            }
        );
    }

    #[test]
    fn a_changed_check_is_incompatible() {
        let catalog =
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER CHECK (v > 0), note TEXT)";
        let live = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER CHECK (v > -5), note TEXT)";
        let (catalog, live) = sqlite(catalog, live);
        assert_eq!(
            judge(&catalog, live.as_ref()),
            Verdict::Incompatible {
                reasons: vec![Incompatibility::CheckChanged {
                    catalog: "v > 0".into(),
                    live: "v > -5".into(),
                }]
            }
        );
    }

    #[test]
    fn a_dropped_check_is_incompatible() {
        let catalog =
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER CHECK (v > 0), note TEXT)";
        let (catalog, live) = sqlite(catalog, BASE);
        assert_eq!(
            judge(&catalog, live.as_ref()),
            Verdict::Incompatible {
                reasons: vec![Incompatibility::CheckDropped {
                    expression: "v > 0".into(),
                }]
            }
        );
    }

    /// SQLite names no `CHECK` constraint, so the judgment compares the
    /// expressions as a multiset: a reordered equal set is unchanged, and
    /// any added, removed or changed expression is reported.
    #[test]
    fn a_reordered_check_set_is_unchanged() {
        let catalog = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER, note TEXT, \
                       CHECK (v > 0), CHECK (note IS NOT NULL))";
        let live = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER, note TEXT, \
                    CHECK (note IS NOT NULL), CHECK (v > 0))";
        let (catalog, live) = sqlite(catalog, live);
        assert_eq!(
            judge(&catalog, live.as_ref()),
            Verdict::Unchanged,
            "the same expressions in another order are one set"
        );
    }

    #[test]
    fn a_removed_duplicated_check_is_reported() {
        let catalog = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER, note TEXT, \
                       CHECK (v > 0), CHECK (v > 0))";
        let live = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER, note TEXT, \
                    CHECK (v > 0))";
        let (catalog, live) = sqlite(catalog, live);
        assert_eq!(
            judge(&catalog, live.as_ref()),
            Verdict::Incompatible {
                reasons: vec![Incompatibility::CheckDropped {
                    expression: "v > 0".into()
                }]
            }
        );
    }

    #[test]
    fn an_added_duplicated_check_is_reported() {
        let catalog = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER, note TEXT, \
                       CHECK (v > 0))";
        let live = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER, note TEXT, \
                    CHECK (v > 0), CHECK (v > 0))";
        let (catalog, live) = sqlite(catalog, live);
        assert_eq!(
            judge(&catalog, live.as_ref()),
            Verdict::Incompatible {
                reasons: vec![Incompatibility::CheckAdded {
                    expression: "v > 0".into()
                }]
            }
        );
    }

    #[test]
    fn a_swapped_duplicated_check_set_is_reported() {
        let catalog = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER, note TEXT, \
                       CHECK (v > 0), CHECK (v > 0), CHECK (note IS NOT NULL))";
        let live = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER, note TEXT, \
                    CHECK (v > 0), CHECK (note IS NOT NULL), CHECK (note IS NOT NULL))";
        let (catalog, live) = sqlite(catalog, live);
        // Each side holds one unmatched expression, and they pair into the
        // one change the multiset difference leaves.
        assert_eq!(
            judge(&catalog, live.as_ref()),
            Verdict::Incompatible {
                reasons: vec![Incompatibility::CheckChanged {
                    catalog: "v > 0".into(),
                    live: "note IS NOT NULL".into()
                }]
            }
        );
    }

    #[test]
    fn a_changed_check_among_unchanged_ones_is_reported() {
        let catalog = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER, note TEXT, \
                       CHECK (v > 0), CHECK (note IS NOT NULL))";
        let live = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER, note TEXT, \
                    CHECK (note IS NOT NULL), CHECK (v > -5))";
        let (catalog, live) = sqlite(catalog, live);
        assert_eq!(
            judge(&catalog, live.as_ref()),
            Verdict::Incompatible {
                reasons: vec![Incompatibility::CheckChanged {
                    catalog: "v > 0".into(),
                    live: "v > -5".into()
                }]
            }
        );
    }

    #[test]
    fn a_changed_primary_key_is_incompatible() {
        let live = "CREATE TABLE t1 (id INTEGER, v INTEGER, note TEXT, PRIMARY KEY (id, v))";
        let (catalog, live) = sqlite(BASE, live);
        // A key column the catalog left nullable is a nullability change
        // beside the key change, and both reasons are reported.
        assert_eq!(
            judge(&catalog, live.as_ref()),
            Verdict::Incompatible {
                reasons: vec![
                    Incompatibility::ColumnNullabilityChanged {
                        column: "v".into(),
                        catalog_nullable: true,
                        live_nullable: false,
                    },
                    Incompatibility::PrimaryKeyChanged {
                        catalog: vec!["id".into()],
                        live: vec!["id".into(), "v".into()],
                    },
                ]
            }
        );
    }

    #[test]
    fn a_missing_table_is_incompatible() {
        let catalog = shape_of::<SQLiteDialect>(BASE);
        assert_eq!(
            judge(&catalog, None),
            Verdict::Incompatible {
                reasons: vec![Incompatibility::TableMissing]
            }
        );
    }

    #[test]
    fn a_weakened_identity_is_a_weakened_verdict() {
        let catalog = shape_of::<SQLiteDialect>(BASE);
        let live = shape_of::<SQLiteDialect>(BASE).with_replica_identity(ReplicaIdentity::Default);
        assert_eq!(
            judge(&catalog, Some(&live)),
            Verdict::IdentityWeakened(ReplicaIdentity::Default)
        );
    }

    #[test]
    fn a_full_identity_is_not_weakened() {
        let catalog = shape_of::<SQLiteDialect>(BASE);
        let live = shape_of::<SQLiteDialect>(BASE).with_replica_identity(ReplicaIdentity::Full);
        assert_eq!(judge(&catalog, Some(&live)), Verdict::Unchanged);
    }

    #[test]
    fn every_reason_survives_in_one_verdict() {
        let live = "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v TEXT, extra INTEGER NOT NULL, PRIMARY KEY (id, extra))";
        let (catalog, live) = sqlite(BASE, live);
        assert_eq!(
            judge(&catalog, live.as_ref()),
            Verdict::Incompatible {
                reasons: vec![
                    Incompatibility::ColumnRetyped {
                        column: "v".into(),
                        catalog: "INT".into(),
                        live: "TEXT".into(),
                    },
                    Incompatibility::ColumnDropped {
                        column: "note".into()
                    },
                    Incompatibility::ColumnNotAddedNullSafe {
                        column: "extra".into()
                    },
                    Incompatibility::PrimaryKeyChanged {
                        catalog: vec!["id".into()],
                        live: vec!["id".into(), "extra".into()],
                    },
                ]
            }
        );
    }

    #[test]
    fn refusal_reasons_follow_the_verdict() {
        let (catalog, live) = sqlite(
            BASE,
            "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER, note TEXT, extra TEXT)",
        );
        assert_eq!(refusal_reasons(&catalog, live.as_ref()), None);
        let (catalog, live) = sqlite(BASE, "CREATE TABLE t1 (id INTEGER PRIMARY KEY, v INTEGER)");
        assert_eq!(
            refusal_reasons(&catalog, live.as_ref()),
            Some(vec![Incompatibility::ColumnDropped {
                column: "note".into()
            }])
        );
    }
}
