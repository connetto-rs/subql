//! Outbound CDC emission: fold ecosystem CDC events into a
//! [`sqlite-diff-rs`](sqlite_diff_rs) patchset.
//!
//! This is the server-to-client half of the round trip. subql ingests
//! generic CDC and emits a [`PatchsetFormat`] patchset directed at the
//! subscribed SQLite clients, which apply it into their local replica.
//! The apply half (client-to-server) lives in [`crate::patchset`].
//!
//! # How it works
//!
//! `sqlite-diff-rs` exposes a schema-driven `digest` entry point:
//! `DiffSetBuilder::digest(event, schema, adapter)` folds one CDC event
//! into a builder, resolving the event's table through a
//! [`WireSchema`] and decoding each column payload through a
//! [`WireAdapter`]. The schema declares one
//! [`WireType`] per column, a source-independent semantic type that
//! selects the decoder. subql's catalog is [`ScalarFamily`]-based, and
//! [`scalar_kind_to_wire_type`] maps one to the other, so a single
//! [`WireCatalog`] over any [`DatabaseLike`] drives every wire source. A
//! decimal column with a declared scale is the one exception: it travels as
//! [`WireType::ScaledDecimal`], an integer count of minor units, which is how
//! a pg2sqlite replica stores a `NUMERIC(p,s)` column.
//!
//! # Scope
//!
//! Three vehicles are wired over one source-agnostic schema side
//! ([`WireCatalog`], [`WireTable`]): [`wal2json_patchset`] digests
//! `wal2json_events` messages, [`maxwell_patchset`] digests `maxwell_cdc`
//! messages, and (behind the `pgoutput-emit` feature) [`pgoutput_patchset`]
//! digests pg_walstream `ChangeEvent`s. Each differs only in its decoder
//! registry.
//!
//! Each vehicle also has a `*_changeset` variant (for example
//! [`wal2json_changeset`]) that emits the changeset format, which records
//! the old and new value of every changed column and so can carry a
//! primary-key-changing UPDATE that a patchset cannot.

use alloc::string::{String, ToString};
use alloc::vec::Vec;

#[cfg(feature = "pgoutput-emit")]
use alloc::sync::Arc;
use hashbrown::HashMap;
use maxwell_cdc::Message as MaxwellMessage;
use sql_traits::prelude::{ColumnLike, DataTypeLike, DatabaseLike, TableLike, VectorKind};
use sqlite_diff_rs::maxwell::{ConversionError as MaxwellConversionError, Maxwell};
#[cfg(feature = "pgoutput-emit")]
use sqlite_diff_rs::pg_walstream::{
    ChangeEvent, ConversionError as PgConversionError, EventType, PgWalstream, RowData,
};
use sqlite_diff_rs::wal2json::{ConversionError, Wal2Json};
use sqlite_diff_rs::{
    ArrayElement, ChangeSet, ChangesetFormat, ColumnNames, DiffFormat, DiffOps, DiffSetBuilder,
    Digestable, DynTable, IndexableValues, Insert, IntervalPostgresDecoder, NamedColumns, PatchSet,
    PatchsetFormat, PgBinary, PgBinaryColumn, SchemaWithPK, SimpleTable, TypeMap,
    UuidBlob16Decoder, Value as WireValue, WireAdapter, WireColumnTypes, WireKind, WireSchema,
    WireType,
};

use crate::backend::ScalarFamily;
use crate::catalog_helpers;
use crate::types::TableId;

/// Map a subql catalog [`ScalarFamily`] to the source-independent
/// [`WireType`] that selects a decoder in `sqlite-diff-rs`'s `digest`.
///
/// The mapping is total: every `ScalarFamily` has a `WireType`.
#[must_use]
pub const fn scalar_kind_to_wire_type(kind: ScalarFamily) -> WireType {
    match kind {
        ScalarFamily::Bool => WireType::Bool,
        ScalarFamily::Int => WireType::Int,
        ScalarFamily::Float => WireType::Real,
        ScalarFamily::String => WireType::Text,
        ScalarFamily::Bytes => WireType::Bytes,
        ScalarFamily::Uuid => WireType::Uuid,
        ScalarFamily::Timestamp => WireType::Timestamp,
        ScalarFamily::TimestampTz => WireType::TimestampTz,
        ScalarFamily::Date => WireType::Date,
        ScalarFamily::Time => WireType::Time,
        ScalarFamily::Decimal => WireType::Decimal,
        ScalarFamily::Json => WireType::Json,
        ScalarFamily::Jsonb => WireType::Jsonb,
        ScalarFamily::Interval => WireType::Interval,
    }
}

/// One subql catalog table viewed as a `sqlite-diff-rs` schema entry.
///
/// Wraps [`SimpleTable`] for the name, column, and primary-key surface
/// and adds one [`WireType`] per column so `digest` selects a decoder
/// without any source-native type key. This is the table type a
/// [`WireCatalog`] resolves each CDC event against.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WireTable {
    inner: SimpleTable,
    wire_types: Vec<WireType>,
}

impl DynTable for WireTable {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn number_of_columns(&self) -> usize {
        self.inner.number_of_columns()
    }

    fn write_pk_flags(&self, buf: &mut [u8]) {
        self.inner.write_pk_flags(buf);
    }
}

impl SchemaWithPK for WireTable {
    fn number_of_primary_keys(&self) -> usize {
        self.inner.number_of_primary_keys()
    }

    fn primary_key_index(&self, col_idx: usize) -> Option<usize> {
        self.inner.primary_key_index(col_idx)
    }

    fn extract_pk<S: Clone, B: Clone>(
        &self,
        values: &impl IndexableValues<Text = S, Binary = B>,
    ) -> Vec<WireValue<S, B>> {
        self.inner.extract_pk(values)
    }
}

impl NamedColumns for WireTable {
    fn column_index(&self, column_name: &str) -> Option<usize> {
        self.inner.column_index(column_name)
    }

    fn column_affinity(&self, column_index: usize) -> Option<sqlite_diff_rs::Affinity> {
        self.inner.column_affinity(column_index)
    }

    fn rowid_alias(&self) -> Option<usize> {
        self.inner.rowid_alias()
    }
}

impl WireColumnTypes for WireTable {
    fn column_type(&self, column_index: usize) -> WireType {
        // The vector holds one entry per column, so an in-range index
        // always hits. The fallback only guards a caller that asks past
        // the column count, which the crate's `digest` never does.
        self.wire_types
            .get(column_index)
            .copied()
            .unwrap_or(WireType::Text)
    }
}

impl ColumnNames for WireTable {
    fn column_name(&self, index: usize) -> Option<&str> {
        self.inner.column_name(index)
    }
}

/// A subql catalog exposed as a `sqlite-diff-rs` [`WireSchema`].
///
/// Holds one owned [`WireTable`] per catalog table, grouped by bare table
/// name and carrying each table's declaring schema. Built once from a
/// [`DatabaseLike`] catalog and reused across a batch of events.
///
/// Lookup is source-qualified: when several tables share a bare name, the
/// event's source schema must name the declaring schema exactly, and an
/// unmatched qualifier refuses rather than guessing. A bare name with one
/// candidate resolves under any qualifier, because a Maxwell event
/// qualifies by MySQL database name, which a PG-parsed catalog cannot
/// know.
#[derive(Debug, Clone)]
pub struct WireCatalog {
    /// Bare table name to declaring schema and table, one entry per table.
    tables: HashMap<String, Vec<(Option<String>, WireTable)>>,
}

impl WireCatalog {
    /// Build a catalog over every table in `database`.
    ///
    /// Columns whose declared SQL type does not map to a [`ScalarFamily`]
    /// fall back to [`WireType::Text`], the lossless affinity for an
    /// unmodeled type on the SQLite side.
    ///
    /// # Errors
    /// [`CatalogError`](crate::CatalogError) when any table's metadata cannot
    /// be resolved: a catalog that cannot describe itself must not emit.
    pub fn from_database<DB: DatabaseLike>(database: &DB) -> Result<Self, crate::CatalogError> {
        let mut tables: HashMap<String, Vec<(Option<String>, WireTable)>> = HashMap::new();
        for index in 0..database.number_of_tables() {
            let Ok(table_id) = TableId::try_from(index) else {
                break;
            };
            let schema = database
                .table_by_id(index)
                .ok_or(crate::CatalogError::UnknownTable(table_id))?
                .table_schema()
                .map(ToString::to_string);
            let wire_table = build_wire_table(database, table_id)?;
            tables
                .entry(wire_table.name().to_string())
                .or_default()
                .push((schema, wire_table));
        }
        Ok(Self { tables })
    }

    /// Number of resolved tables.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tables.values().map(Vec::len).sum()
    }

    /// True when no table resolved.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }
}

impl WireSchema for WireCatalog {
    type Table = WireTable;

    fn get(&self, source_schema: Option<&str>, table_name: &str) -> Option<&WireTable> {
        let candidates = self.tables.get(table_name)?;
        let exact = candidates
            .iter()
            .find(|(schema, _)| schema.as_deref() == source_schema)
            .map(|(_, table)| table);
        if exact.is_some() {
            return exact;
        }
        // One candidate is unambiguous under any qualifier.
        match candidates.as_slice() {
            [(_, only)] => Some(only),
            _ => None,
        }
    }
}

/// The [`WireType`] `column` travels as, the form a pg2sqlite replica
/// stores it in, or `None` when no replica can hold it.
fn column_wire_type<DB: DatabaseLike>(database: &DB, column: &DB::Column) -> Option<WireType> {
    let data_type = column.data_type(database);
    if let Some(element) = data_type.array_element(database) {
        return array_element(database, element).map(|element| WireType::Array { element });
    }
    if let Some(vector) = data_type.vector(database) {
        // sqlite-vec stores single-precision vectors only.
        return (vector.kind() == VectorKind::Vector).then(|| WireType::Vector {
            dimensions: vector.dimensions(),
        });
    }
    data_type
        .scalar_family(database)
        .map(|family| match family {
            ScalarFamily::Decimal => catalog_helpers::minor_unit_scale(database, data_type)
                .map_or(WireType::Decimal, |scale| WireType::ScaledDecimal { scale }),
            other => scalar_kind_to_wire_type(other),
        })
}

/// The [`ArrayElement`] an array of `element` travels as, or `None` when the
/// element is BLOB-stored or minor units, which a JSON array cannot hold.
fn array_element<DB: DatabaseLike>(database: &DB, element: &DB::DataType) -> Option<ArrayElement> {
    if element.vector(database).is_some() {
        return None;
    }
    let Some(family) = element.scalar_family(database) else {
        return Some(ArrayElement::Text);
    };
    Some(match family {
        ScalarFamily::Bool => ArrayElement::Bool,
        ScalarFamily::Int => ArrayElement::Int,
        ScalarFamily::Float => ArrayElement::Real,
        ScalarFamily::Decimal => {
            if catalog_helpers::minor_unit_scale(database, element).is_some() {
                return None;
            }
            ArrayElement::Decimal
        }
        ScalarFamily::String => ArrayElement::Text,
        ScalarFamily::Bytes | ScalarFamily::Uuid => return None,
        ScalarFamily::Date => ArrayElement::Date,
        ScalarFamily::Time => ArrayElement::Time,
        ScalarFamily::Timestamp => ArrayElement::Timestamp,
        ScalarFamily::TimestampTz => ArrayElement::TimestampTz,
        ScalarFamily::Json => ArrayElement::Json,
        ScalarFamily::Jsonb => ArrayElement::Jsonb,
        ScalarFamily::Interval => ArrayElement::Interval,
    })
}

/// Build one [`WireTable`] for `table_id`.
fn build_wire_table<DB: DatabaseLike>(
    database: &DB,
    table_id: TableId,
) -> Result<WireTable, crate::CatalogError> {
    let inner = catalog_helpers::simple_table(database, table_id)?;
    let wire_types = catalog_helpers::table_columns(database, table_id)?
        .map(|column| column_wire_type(database, column).unwrap_or(WireType::Text))
        .collect();
    Ok(WireTable { inner, wire_types })
}

/// Fold `events` into one diff set of format `F` over `database`, resolving
/// each event's table through a [`WireCatalog`] and decoding its columns
/// through `adapter`.
///
/// Resolve `database` into a [`WireCatalog`], the one spelling of the
/// catalog failure that every vehicle below shares.
fn wire_catalog<DB: DatabaseLike>(database: &DB) -> Result<WireCatalog, ConversionError> {
    WireCatalog::from_database(database).map_err(|e| ConversionError::TableNotFound(e.to_string()))
}

/// The one procedure behind every vehicle below: the format, the wire source
/// and the decoder registry are the only things that differ. Every source in
/// `sqlite-diff-rs` 0.12 reports the same [`ConversionError`], so the catalog
/// failure has one spelling here rather than one per vehicle.
fn fold_events<'a, F, E>(
    catalog: &WireCatalog,
    adapter: &impl WireAdapter<E::Src, String, Vec<u8>>,
    events: impl IntoIterator<Item = &'a E>,
) -> Result<DiffSetBuilder<F, WireTable, String, Vec<u8>>, ConversionError>
where
    F: DiffFormat<String, Vec<u8>>,
    E: Digestable<F, WireTable, String, Vec<u8>, Error = ConversionError> + 'a,
{
    let mut builder = DiffSetBuilder::<F, WireTable, String, Vec<u8>>::new();
    for event in events {
        builder = builder.digest(event, catalog, adapter)?;
    }
    Ok(builder)
}

/// Rebuild `row` without the names `table` does not know, or `None` when
/// `row` names only columns `table` has. The check walks the row's own
/// names, so a row the catalog fully knows allocates nothing.
#[cfg(feature = "pgoutput-emit")]
fn project_row(row: &RowData, table: &WireTable) -> Option<RowData> {
    let unknown = row
        .iter()
        .any(|(name, _)| table.column_index(name).is_none());
    if !unknown {
        return None;
    }
    let mut projected = RowData::new();
    for (name, value) in row.iter() {
        if table.column_index(name).is_some() {
            projected.push(Arc::clone(name), value.clone());
        }
    }
    Some(projected)
}

/// Project a row event onto the columns of its table in `catalog`, returning
/// the rebuilt event when a row names a column the catalog's table lacks and
/// `None` when the event folds as is.
///
/// The fold is strict about columns its table lacks, which is the right
/// contract for the library. Projecting onto subql's own catalog is subql's
/// policy for a server built from an older schema whose database picked up a
/// column afterwards. A table the catalog lacks stays for the fold to
/// report, and destructive changes are not this path's concern, since
/// connetto detects them from the `Relation` item and refuses the table
/// first.
#[cfg(feature = "pgoutput-emit")]
fn project_event(event: &EventType, catalog: &WireCatalog) -> Option<EventType> {
    match event {
        EventType::Insert {
            schema,
            table,
            relation_oid,
            data,
        } => {
            let wire = catalog.get(Some(schema.as_ref()), table)?;
            let data = project_row(data, wire)?;
            Some(EventType::Insert {
                schema: Arc::clone(schema),
                table: Arc::clone(table),
                relation_oid: *relation_oid,
                data,
            })
        }
        EventType::Update {
            schema,
            table,
            relation_oid,
            old_data,
            new_data,
            replica_identity,
            key_columns,
        } => {
            let wire = catalog.get(Some(schema.as_ref()), table)?;
            let new_data = project_row(new_data, wire)?;
            let old_data = old_data.as_ref().and_then(|row| project_row(row, wire));
            Some(EventType::Update {
                schema: Arc::clone(schema),
                table: Arc::clone(table),
                relation_oid: *relation_oid,
                old_data,
                new_data,
                replica_identity: *replica_identity,
                key_columns: key_columns
                    .iter()
                    .filter(|name| wire.column_index(name).is_some())
                    .cloned()
                    .collect(),
            })
        }
        EventType::Delete {
            schema,
            table,
            relation_oid,
            old_data,
            replica_identity,
            key_columns,
        } => {
            let wire = catalog.get(Some(schema.as_ref()), table)?;
            let old_data = project_row(old_data, wire)?;
            Some(EventType::Delete {
                schema: Arc::clone(schema),
                table: Arc::clone(table),
                relation_oid: *relation_oid,
                old_data,
                replica_identity: *replica_identity,
                key_columns: key_columns
                    .iter()
                    .filter(|name| wire.column_index(name).is_some())
                    .cloned()
                    .collect(),
            })
        }
        _ => None,
    }
}

/// Fold a batch of pgoutput events over `catalog`, projecting each event's
/// rows onto the catalog's columns first, through `adapter`.
///
/// The pg builders project before folding because the fold is strict about
/// columns its table lacks. The common event, whose rows the catalog fully
/// knows, folds as is, and the projected event borrows from the batch-local
/// buffer until its digest, which one buffer serves.
#[cfg(feature = "pgoutput-emit")]
fn fold_pg_events<'a, F>(
    catalog: &WireCatalog,
    adapter: &impl WireAdapter<
        <EventType as Digestable<F, WireTable, String, Vec<u8>>>::Src,
        String,
        Vec<u8>,
    >,
    events: impl IntoIterator<Item = &'a EventType>,
) -> Result<DiffSetBuilder<F, WireTable, String, Vec<u8>>, ConversionError>
where
    F: DiffFormat<String, Vec<u8>>,
    EventType: Digestable<F, WireTable, String, Vec<u8>, Error = ConversionError>,
{
    let mut builder = DiffSetBuilder::<F, WireTable, String, Vec<u8>>::new();
    let mut cache: Vec<EventType> = Vec::new();
    for event in events {
        let event = project_event(event, catalog).map_or(event, |projected| {
            cache.push(projected);
            cache.last().expect("the projected event was just pushed")
        });
        builder = builder.digest(event, catalog, adapter)?;
    }
    Ok(builder)
}

/// The wal2json decoder registry subql feeds to `digest`.
///
/// `sqlite-diff-rs` keys the registry on [`WireKind`], so one
/// `defaults()` table decodes every wal2json column subql models. subql
/// overrides two decoders. A `UUID` column lowers to a compact 16-byte
/// `Value::Blob` through [`UuidBlob16Decoder`] rather than the 36-char text
/// form, since the SQLite client stores UUIDs as BLOBs and `PgAdapter`
/// re-binds the blob to a native Postgres `UUID` on the way back. An
/// `interval` lowers through [`IntervalPostgresDecoder`] to the text
/// PostgreSQL prints, such as `00:15:00`, which is what a pg2sqlite replica
/// stores, where the default decoder would write ISO 8601.
#[must_use]
pub fn wal2json_adapter() -> TypeMap<Wal2Json, String, Vec<u8>> {
    TypeMap::defaults()
        .with(WireKind::Uuid, UuidBlob16Decoder)
        .with(WireKind::Interval, IntervalPostgresDecoder)
}

/// Fold a batch of wal2json CDC events into one [`PatchsetFormat`]
/// patchset builder over `database`.
///
/// Each event is resolved to its table through a [`WireCatalog`] built
/// from `database` and decoded through [`wal2json_adapter`].
/// Transaction-boundary messages (begin, commit, message, truncate)
/// digest to no-ops. The builder is a [`PatchSet`], so it both serializes
/// to wire bytes via `build()` (see [`wal2json_patchset`]) and applies
/// directly through [`crate::patchset`]'s `SqliteAdapter`.
///
/// Accepts either `MessageV2` (wal2json v2) or `ChangeV1` (v1) events,
/// since both implement the wal2json `Digestable` contract.
///
/// # Errors
///
/// Propagates [`ConversionError`] when an event names a table or column
/// absent from `database`, or a decoder rejects a column payload.
pub fn wal2json_patchset_builder<DB, E>(
    database: &DB,
    events: &[E],
) -> Result<PatchSet<WireTable, String, Vec<u8>>, ConversionError>
where
    DB: DatabaseLike,
    E: Digestable<
        PatchsetFormat,
        WireTable,
        String,
        Vec<u8>,
        Src = Wal2Json,
        Error = ConversionError,
    >,
{
    let catalog = wire_catalog(database)?;
    fold_events(&catalog, &wal2json_adapter(), events)
}

/// Fold a batch of wal2json CDC events into one [`PatchsetFormat`]
/// patchset over `database`, returning the wire bytes.
///
/// A thin wrapper over [`wal2json_patchset_builder`] that serializes the
/// builder with `build()`. The returned bytes carry the SQLite
/// session-extension patchset marker and apply through any
/// `sqlite-diff-rs` consumer.
///
/// # Errors
///
/// Propagates [`ConversionError`], as [`wal2json_patchset_builder`] does.
pub fn wal2json_patchset<DB, E>(database: &DB, events: &[E]) -> Result<Vec<u8>, ConversionError>
where
    DB: DatabaseLike,
    E: Digestable<
        PatchsetFormat,
        WireTable,
        String,
        Vec<u8>,
        Src = Wal2Json,
        Error = ConversionError,
    >,
{
    Ok(wal2json_patchset_builder(database, events)?.build())
}

/// Fold a batch of wal2json CDC events into one [`ChangesetFormat`]
/// changeset builder over `database`, the changeset counterpart to
/// [`wal2json_patchset_builder`].
///
/// A changeset records the old and new value of every changed column, so
/// unlike a patchset it can carry a primary-key-changing UPDATE. The old
/// values come from each event's old-row image (`identity`), so full
/// fidelity needs `REPLICA IDENTITY FULL`. Accepts either `MessageV2` or
/// `ChangeV1`.
///
/// # Errors
///
/// Propagates [`ConversionError`] when an event names a table or column
/// absent from `database`, or a decoder rejects a column payload.
pub fn wal2json_changeset_builder<DB, E>(
    database: &DB,
    events: &[E],
) -> Result<ChangeSet<WireTable, String, Vec<u8>>, ConversionError>
where
    DB: DatabaseLike,
    E: Digestable<
        ChangesetFormat,
        WireTable,
        String,
        Vec<u8>,
        Src = Wal2Json,
        Error = ConversionError,
    >,
{
    let catalog = wire_catalog(database)?;
    fold_events(&catalog, &wal2json_adapter(), events)
}

/// Fold a batch of wal2json CDC events into one [`ChangesetFormat`]
/// changeset over `database`, returning the wire bytes.
///
/// A thin wrapper over [`wal2json_changeset_builder`] that serializes with
/// `build()`.
///
/// # Errors
///
/// Propagates [`ConversionError`], as [`wal2json_changeset_builder`] does.
pub fn wal2json_changeset<DB, E>(database: &DB, events: &[E]) -> Result<Vec<u8>, ConversionError>
where
    DB: DatabaseLike,
    E: Digestable<
        ChangesetFormat,
        WireTable,
        String,
        Vec<u8>,
        Src = Wal2Json,
        Error = ConversionError,
    >,
{
    Ok(wal2json_changeset_builder(database, events)?.build())
}

/// The pgoutput decoder registry subql feeds to `digest`.
///
/// Requires the `pgoutput-emit` feature. Overrides UUID to
/// [`UuidBlob16Decoder`] and `interval` to [`IntervalPostgresDecoder`],
/// matching [`wal2json_adapter`] and the SQLite client's storage.
#[cfg(feature = "pgoutput-emit")]
#[must_use]
pub fn pgoutput_adapter() -> TypeMap<PgWalstream, String, Vec<u8>> {
    TypeMap::defaults()
        .with(WireKind::Uuid, UuidBlob16Decoder)
        .with(WireKind::Interval, IntervalPostgresDecoder)
}

/// Fold a batch of pgoutput `ChangeEvent`s into one [`PatchsetFormat`]
/// patchset builder over `database`, mirroring [`wal2json_patchset_builder`].
///
/// `sqlite-diff-rs` implements the pg `Digestable` on
/// `pg_walstream::EventType`, so each event's `event_type` folds into the
/// builder over the same source-agnostic [`WireCatalog`]. Non-row events
/// (begin, commit, relation, and similar) digest to no-ops.
///
/// # Errors
///
/// Propagates the pg source `ConversionError` when an event names a table
/// or column absent from `database`, or a decoder rejects a payload.
#[cfg(feature = "pgoutput-emit")]
pub fn pgoutput_patchset_builder<DB: DatabaseLike>(
    database: &DB,
    events: &[ChangeEvent],
) -> Result<PatchSet<WireTable, String, Vec<u8>>, PgConversionError> {
    let catalog = wire_catalog(database)?;
    fold_pg_events(
        &catalog,
        &pgoutput_adapter(),
        events.iter().map(|event| &event.event_type),
    )
}

/// Serialize [`pgoutput_patchset_builder`] to wire bytes.
///
/// # Errors
///
/// Propagates the pg source `ConversionError`.
#[cfg(feature = "pgoutput-emit")]
pub fn pgoutput_patchset<DB: DatabaseLike>(
    database: &DB,
    events: &[ChangeEvent],
) -> Result<Vec<u8>, PgConversionError> {
    Ok(pgoutput_patchset_builder(database, events)?.build())
}

/// Fold a batch of pgoutput `ChangeEvent`s into one [`ChangesetFormat`]
/// changeset builder over `database`, the changeset counterpart to
/// [`pgoutput_patchset_builder`].
///
/// A changeset records old and new values, so it can carry a
/// primary-key-changing UPDATE. The old values come from each event's old
/// tuple, so full fidelity needs `REPLICA IDENTITY FULL`.
///
/// # Errors
///
/// Propagates the pg source `ConversionError` when an event names a table
/// or column absent from `database`, or a decoder rejects a payload.
#[cfg(feature = "pgoutput-emit")]
pub fn pgoutput_changeset_builder<DB: DatabaseLike>(
    database: &DB,
    events: &[ChangeEvent],
) -> Result<ChangeSet<WireTable, String, Vec<u8>>, PgConversionError> {
    let catalog = wire_catalog(database)?;
    fold_pg_events(
        &catalog,
        &pgoutput_adapter(),
        events.iter().map(|event| &event.event_type),
    )
}

/// Serialize [`pgoutput_changeset_builder`] to wire bytes.
///
/// # Errors
///
/// Propagates the pg source `ConversionError`.
#[cfg(feature = "pgoutput-emit")]
pub fn pgoutput_changeset<DB: DatabaseLike>(
    database: &DB,
    events: &[ChangeEvent],
) -> Result<Vec<u8>, PgConversionError> {
    Ok(pgoutput_changeset_builder(database, events)?.build())
}

/// The Maxwell decoder registry subql feeds to `digest`.
///
/// MySQL has no native UUID type, so a UUID stored as `BINARY(16)`
/// classifies as [`ScalarFamily::Bytes`] and rides [`WireType::Bytes`],
/// which the default `MySqlBinaryDecoder` base64-decodes to a compact
/// 16-byte `Value::Blob`. That matches the SQLite client's blob storage
/// and the way subql's `MysqlAdapter` rebinds the blob as MySQL binary,
/// so the defaults need no override.
#[must_use]
pub fn maxwell_adapter() -> TypeMap<Maxwell, String, Vec<u8>> {
    TypeMap::defaults()
}

/// Fold a batch of Maxwell `Message`s into one [`PatchsetFormat`]
/// patchset builder over `database`, mirroring [`wal2json_patchset_builder`].
///
/// Each row message folds into the builder over the same source-agnostic
/// [`WireCatalog`]. Control messages carry no row data and should be
/// dropped before this call (subql's `parse_maxwell` does so).
///
/// # Errors
///
/// Propagates the Maxwell source `ConversionError` when a message names a
/// table or column absent from `database`, or a decoder rejects a payload.
pub fn maxwell_patchset_builder<DB: DatabaseLike>(
    database: &DB,
    events: &[MaxwellMessage],
) -> Result<PatchSet<WireTable, String, Vec<u8>>, MaxwellConversionError> {
    let catalog = wire_catalog(database)?;
    fold_events(&catalog, &maxwell_adapter(), events)
}

/// Serialize [`maxwell_patchset_builder`] to wire bytes.
///
/// # Errors
///
/// Propagates the Maxwell source `ConversionError`.
pub fn maxwell_patchset<DB: DatabaseLike>(
    database: &DB,
    events: &[MaxwellMessage],
) -> Result<Vec<u8>, MaxwellConversionError> {
    Ok(maxwell_patchset_builder(database, events)?.build())
}

/// Fold a batch of Maxwell `Message`s into one [`ChangesetFormat`]
/// changeset builder over `database`, the changeset counterpart to
/// [`maxwell_patchset_builder`].
///
/// A changeset records old and new values, so it can carry a
/// primary-key-changing UPDATE. The old values come from each message's
/// `old` field, which MySQL populates for the changed columns.
///
/// # Errors
///
/// Propagates the Maxwell source `ConversionError` when a message names a
/// table or column absent from `database`, or a decoder rejects a payload.
pub fn maxwell_changeset_builder<DB: DatabaseLike>(
    database: &DB,
    events: &[MaxwellMessage],
) -> Result<ChangeSet<WireTable, String, Vec<u8>>, MaxwellConversionError> {
    let catalog = wire_catalog(database)?;
    fold_events(&catalog, &maxwell_adapter(), events)
}

/// Serialize [`maxwell_changeset_builder`] to wire bytes.
///
/// # Errors
///
/// Propagates the Maxwell source `ConversionError`.
pub fn maxwell_changeset<DB: DatabaseLike>(
    database: &DB,
    events: &[MaxwellMessage],
) -> Result<Vec<u8>, MaxwellConversionError> {
    Ok(maxwell_changeset_builder(database, events)?.build())
}

/// The `PgBinary` decoder registry for snapshot reads.
///
/// The snapshot counterpart to [`pgoutput_adapter`]. The `PgBinary`
/// `defaults()` already map [`WireKind::Uuid`] to [`UuidBlob16Decoder`], so
/// only `interval` is overridden, to [`IntervalPostgresDecoder`]. Every
/// column then lowers byte-identically to the pgoutput CDC path.
#[must_use]
pub fn pgbinary_adapter() -> TypeMap<PgBinary, String, Vec<u8>> {
    TypeMap::defaults().with(WireKind::Interval, IntervalPostgresDecoder)
}

/// Encode Postgres binary result rows for `table` into an insert-patchset
/// builder, the snapshot counterpart to [`pgoutput_patchset_builder`].
///
/// `column_names` names the result columns in order. Each row in `rows`
/// carries one `Option<&[u8]>` per result column, the raw Postgres binary
/// bytes (or `None` for SQL NULL), aligned to `column_names`. Column order
/// and primary key come from the catalog, and each column's [`WireType`] is
/// the catalog's (a scaled decimal as [`WireType::ScaledDecimal`]), the
/// same source the pgoutput CDC path folds through, so the patchset is
/// byte-identical to that path for the same rows (a `uuid` lands as a
/// 16-byte blob). A catalog column absent from `column_names` is stored as
/// NULL.
///
/// A snapshot is a set of rows to insert with no old images, so this
/// assembles [`Insert`]s directly rather than using the `Digestable` fold
/// the CDC vehicles need for old-image-carrying ops.
///
/// # Errors
///
/// [`ConversionError::TableNotFound`] when `table` is not in the catalog,
/// [`ConversionError::UnsupportedType`] when a catalog column's declared
/// type has no [`ScalarFamily`], [`ConversionError::MissingColumns`] when a
/// row's width does not match `column_names`, and
/// [`ConversionError::Decode`] when a column's bytes do not decode for its
/// type.
pub fn pgbinary_patchset_builder<B: crate::backend::Backend, DB: DatabaseLike>(
    database: &DB,
    table: &str,
    column_names: &[&str],
    rows: &[Vec<Option<&[u8]>>],
) -> Result<PatchSet<SimpleTable, String, Vec<u8>>, ConversionError> {
    let table_id = catalog_helpers::table_id::<B, DB>(database, table)
        .ok_or_else(|| ConversionError::TableNotFound(table.to_string()))?;
    let simple = catalog_helpers::simple_table(database, table_id)
        .map_err(|e| ConversionError::TableNotFound(e.to_string()))?;

    // The same catalog columns the CDC path folds through, so the two encoders agree.
    let mut columns: Vec<(String, WireType)> = Vec::with_capacity(simple.number_of_columns());
    for column in catalog_helpers::table_columns(database, table_id)
        .map_err(|e| ConversionError::TableNotFound(e.to_string()))?
    {
        let name = column.column_name().to_string();
        let wire_type = column_wire_type(database, column)
            .ok_or_else(|| ConversionError::UnsupportedType(name.clone()))?;
        columns.push((name, wire_type));
    }

    // Map each catalog column name to its position in the result row.
    let mut positions: HashMap<&str, usize> = HashMap::with_capacity(column_names.len());
    for (idx, name) in column_names.iter().enumerate() {
        positions.insert(*name, idx);
    }

    let adapter = pgbinary_adapter();
    let mut builder = PatchSet::<SimpleTable, String, Vec<u8>>::new();
    for row in rows {
        if row.len() != column_names.len() {
            return Err(ConversionError::MissingColumns);
        }
        let mut insert = Insert::<SimpleTable, String, Vec<u8>>::from(simple.clone());
        for (ordinal, (name, wire_type)) in columns.iter().enumerate() {
            let raw = positions.get(name.as_str()).and_then(|&idx| row[idx]);
            let value = adapter.decode(PgBinaryColumn {
                column_name: name,
                wire_type: *wire_type,
                raw,
            })?;
            // `ordinal` is bounded by the catalog arity that sized the
            // insert, so `set` cannot report an out-of-bounds column.
            insert = insert
                .set(ordinal, value)
                .map_err(|_| ConversionError::MissingColumns)?;
        }
        builder = builder.insert(insert);
    }
    Ok(builder)
}

/// Encode Postgres binary result rows for `table` into an insert-patchset,
/// the snapshot counterpart to [`pgoutput_patchset`].
///
/// A thin wrapper over [`pgbinary_patchset_builder`] that serializes with
/// `build()`. An empty `rows` yields an empty patchset (zero bytes),
/// matching the CDC vehicles.
///
/// # Errors
///
/// Propagates [`ConversionError`], as [`pgbinary_patchset_builder`] does.
pub fn pgbinary_patchset<B: crate::backend::Backend, DB: DatabaseLike>(
    database: &DB,
    table: &str,
    column_names: &[&str],
    rows: &[Vec<Option<&[u8]>>],
) -> Result<Vec<u8>, ConversionError> {
    Ok(pgbinary_patchset_builder::<B, DB>(database, table, column_names, rows)?.build())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use sql_traits::structs::ParserDB;
    use sqlite_diff_rs::{ChangesetOp, ParsedDiffSet, PatchsetOp, TableSchema};
    use sqlparser::dialect::PostgreSqlDialect;
    use wal2json_events::parse_v2;

    fn orders_db() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, amount INT, status TEXT);",
        )
        .unwrap()
    }

    /// The wal2json line's patchset bytes, with the marker asserted. The
    /// emitted ops borrow from the returned value, so the caller still
    /// matches the `Patchset` variant.
    fn patchset_for_line(db: &ParserDB, line: &str) -> ParsedDiffSet {
        let msg = parse_v2(line).unwrap();
        let bytes = wal2json_patchset(db, core::slice::from_ref(&msg)).unwrap();
        parse_patchset(&bytes)
    }

    /// Owns the parsed patchset of one wal2json line so a test borrows
    /// its ops without restating the variant unwrap and collect.
    struct PatchsetOps {
        parsed: ParsedDiffSet,
    }

    /// The op view of a parsed patchset, spelled once for the test bodies.
    type RowOp<'a> = PatchsetOp<'a, TableSchema<String>, String, Vec<u8>>;

    impl PatchsetOps {
        fn ops(&self) -> Vec<RowOp<'_>> {
            let ParsedDiffSet::Patchset(diff) = &self.parsed else {
                unreachable!("parse_patchset asserted the Patchset variant");
            };
            diff.iter().collect()
        }
    }

    fn patchset_ops(db: &ParserDB, line: &str) -> PatchsetOps {
        PatchsetOps {
            parsed: patchset_for_line(db, line),
        }
    }

    /// The pk of the one update op, panicking with the op's shape otherwise.
    fn update_pk(ops: &[RowOp<'_>]) -> Vec<WireValue<String, Vec<u8>>> {
        let PatchsetOp::Update { pk, .. } = &ops[0] else {
            panic!("expected an update op, got {:?}", ops[0]);
        };
        pk.to_vec()
    }

    /// The pk of the one delete op, panicking with the op's shape otherwise.
    fn delete_pk(ops: &[RowOp<'_>]) -> Vec<WireValue<String, Vec<u8>>> {
        let PatchsetOp::Delete { pk, .. } = &ops[0] else {
            panic!("expected a delete op, got {:?}", ops[0]);
        };
        pk.to_vec()
    }

    fn parse_patchset(bytes: &[u8]) -> ParsedDiffSet {
        let parsed = ParsedDiffSet::parse(bytes).unwrap();
        assert!(parsed.is_patchset(), "expected the SQLite patchset marker");
        parsed
    }

    #[test]
    fn scalar_kind_maps_cover_every_kind() {
        // A change to `ScalarFamily` must be reflected here. The match in
        // `scalar_kind_to_wire_type` is exhaustive, so this only pins the
        // representative mappings the wal2json decoders depend on.
        assert_eq!(scalar_kind_to_wire_type(ScalarFamily::Uuid), WireType::Uuid);
        assert_eq!(
            scalar_kind_to_wire_type(ScalarFamily::Float),
            WireType::Real
        );
        assert_eq!(
            scalar_kind_to_wire_type(ScalarFamily::Jsonb),
            WireType::Jsonb
        );
    }

    #[test]
    fn wire_catalog_resolves_declared_table() {
        let db = orders_db();
        let catalog = WireCatalog::from_database(&db).expect("catalog resolves");
        assert_eq!(catalog.len(), 1);
        let table = catalog.get(None, "orders").expect("orders resolves");
        assert_eq!(table.number_of_columns(), 3);
        assert_eq!(table.column_type(0), WireType::Int);
        assert_eq!(table.column_type(2), WireType::Text);
        assert!(catalog.get(None, "missing").is_none());
    }

    #[test]
    fn wire_tables_record_sql_literals_without_declared_type_coercion() {
        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount NUMERIC, note TEXT);",
        )
        .unwrap();
        let catalog = WireCatalog::from_database(&db).expect("catalog resolves");
        let mut builder = PatchSet::<WireTable, String, Vec<u8>>::new();
        builder.add_table(catalog.get(None, "ledger").expect("ledger resolves"));
        builder
            .digest_sql("INSERT INTO ledger (id, amount, note) VALUES ('k1', '12.50', 7)")
            .expect("an untyped key is no rowid alias");
        let parsed = PatchsetOps {
            parsed: parse_patchset(&builder.build()),
        };
        let ops = parsed.ops();
        let PatchsetOp::Insert { values, .. } = &ops[0] else {
            panic!("expected an insert op, got {:?}", ops[0]);
        };
        assert_eq!(
            values.to_vec(),
            vec![
                WireValue::Text("k1".into()),
                WireValue::Text("12.50".into()),
                WireValue::Integer(7),
            ]
        );
    }

    #[test]
    fn insert_emits_patchset_insert_with_row_values() {
        let db = orders_db();
        let line = r#"{"action":"I","schema":"public","table":"orders","columns":[{"name":"id","type":"integer","value":1},{"name":"amount","type":"integer","value":100},{"name":"status","type":"text","value":"new"}]}"#;
        let ps = patchset_ops(&db, line);
        let ops = ps.ops();
        assert_eq!(ops.len(), 1);
        let PatchsetOp::Insert { table, values, .. } = &ops[0] else {
            panic!("expected an insert op, got {:?}", ops[0]);
        };
        assert_eq!(table.name(), "orders");
        assert_eq!(
            values.to_vec(),
            vec![
                WireValue::Integer(1),
                WireValue::Integer(100),
                WireValue::Text("new".to_string()),
            ]
        );
    }

    #[test]
    fn update_emits_patchset_update_with_pk_and_new_values() {
        let db = orders_db();
        let line = r#"{"action":"U","schema":"public","table":"orders","columns":[{"name":"id","type":"integer","value":1},{"name":"amount","type":"integer","value":250},{"name":"status","type":"text","value":"shipped"}],"identity":[{"name":"id","type":"integer","value":1}]}"#;
        let ps = patchset_ops(&db, line);
        let ops = ps.ops();
        assert_eq!(ops.len(), 1);
        let PatchsetOp::Update { table, pk, .. } = &ops[0] else {
            panic!("expected an update op, got {:?}", ops[0]);
        };
        assert_eq!(table.name(), "orders");
        assert_eq!(pk.to_vec(), vec![WireValue::Integer(1)]);
        // The new non-PK values ride along as the patchset's new image.
        let new_values: Vec<_> = ops[0]
            .update_new_values()
            .expect("update carries new values");
        assert!(new_values
            .iter()
            .any(|v| matches!(v, Some(WireValue::Text(s)) if s == "shipped")));
    }

    #[test]
    fn delete_emits_patchset_delete_with_pk_only() {
        let db = orders_db();
        let line = r#"{"action":"D","schema":"public","table":"orders","identity":[{"name":"id","type":"integer","value":7}]}"#;
        let ps = patchset_ops(&db, line);
        let ops = ps.ops();
        assert_eq!(ops.len(), 1);
        let PatchsetOp::Delete { table, pk, .. } = &ops[0] else {
            panic!("expected a delete op, got {:?}", ops[0]);
        };
        assert_eq!(table.name(), "orders");
        assert_eq!(pk.to_vec(), vec![WireValue::Integer(7)]);
    }

    #[test]
    fn transaction_boundaries_digest_to_empty_patchset() {
        let db = orders_db();
        let begin = parse_v2(r#"{"action":"B"}"#).unwrap();
        let commit = parse_v2(r#"{"action":"C"}"#).unwrap();

        // A batch of only transaction boundaries carries no row ops, so
        // the builder is empty and `build()` emits zero bytes.
        let bytes = wal2json_patchset(&db, &[begin, commit]).unwrap();
        assert!(
            bytes.is_empty(),
            "begin and commit carry no row ops so no bytes are emitted"
        );
    }

    #[test]
    fn unknown_table_is_an_error() {
        let db = orders_db();
        let line = r#"{"action":"I","schema":"public","table":"absent","columns":[{"name":"id","type":"integer","value":1}]}"#;
        let msg = parse_v2(line).unwrap();

        let err = wal2json_patchset(&db, core::slice::from_ref(&msg)).unwrap_err();
        assert!(matches!(err, ConversionError::TableNotFound(name) if name == "absent"));
    }

    fn pairs_db() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE pairs (a INT, b INT, v TEXT, PRIMARY KEY (a, b));",
        )
        .unwrap()
    }

    /// Under default replica identity the old-row image an UPDATE carries
    /// is key-only, and a DELETE carries only the key. The patchset update
    /// digest builds its WHERE from the primary key in the new image
    /// (`columns`) and never reads the old image, so a key-only old image
    /// is sufficient. This pins that the loop does not require
    /// `REPLICA IDENTITY FULL`.
    #[test]
    fn default_replica_identity_key_only_old_image_emits_update_and_delete() {
        let db = orders_db();

        // UPDATE with a full new image and a key-only identity (default RI).
        let update = r#"{"action":"U","schema":"public","table":"orders","columns":[{"name":"id","type":"integer","value":1},{"name":"amount","type":"integer","value":250},{"name":"status","type":"text","value":"shipped"}],"identity":[{"name":"id","type":"integer","value":1}]}"#;
        let ps = patchset_ops(&db, update);
        let ops = ps.ops();
        assert_eq!(
            update_pk(&ops),
            vec![WireValue::Integer(1)],
            "WHERE key present"
        );
        let new_values = ops[0]
            .update_new_values()
            .expect("update carries new values");
        assert!(new_values
            .iter()
            .any(|v| matches!(v, Some(WireValue::Text(s)) if s == "shipped")));

        // DELETE with a key-only identity (default RI).
        let delete = r#"{"action":"D","schema":"public","table":"orders","identity":[{"name":"id","type":"integer","value":1}]}"#;
        let ps = patchset_ops(&db, delete);
        let ops = ps.ops();
        assert_eq!(
            delete_pk(&ops),
            vec![WireValue::Integer(1)],
            "delete matches key"
        );
    }

    /// A composite primary key produces an update and a delete whose pk
    /// carries every key column, in pk-ordinal order, so the apply side
    /// can match on all of them.
    #[test]
    fn composite_pk_update_and_delete_carry_all_key_columns() {
        let db = pairs_db();

        let update = r#"{"action":"U","schema":"public","table":"pairs","columns":[{"name":"a","type":"integer","value":1},{"name":"b","type":"integer","value":2},{"name":"v","type":"text","value":"y2"}],"identity":[{"name":"a","type":"integer","value":1},{"name":"b","type":"integer","value":2}]}"#;
        let ps = patchset_ops(&db, update);
        let ops = ps.ops();
        assert_eq!(
            update_pk(&ops),
            vec![WireValue::Integer(1), WireValue::Integer(2)],
            "both key columns in the update WHERE"
        );

        let delete = r#"{"action":"D","schema":"public","table":"pairs","identity":[{"name":"a","type":"integer","value":1},{"name":"b","type":"integer","value":2}]}"#;
        let ps = patchset_ops(&db, delete);
        let ops = ps.ops();
        assert_eq!(
            delete_pk(&ops),
            vec![WireValue::Integer(1), WireValue::Integer(2)],
            "both key columns in the delete WHERE"
        );
    }

    /// A primary-key-changing UPDATE cannot round-trip through a patchset:
    /// the digest builds the WHERE from the new image, so the emitted op
    /// targets the new key and the old key is lost. Applied to a replica
    /// that still holds the old key, it matches no row, so the replica
    /// diverges. This pins the documented limitation from the emit side.
    #[test]
    fn pk_changing_update_targets_the_new_key_documenting_the_limitation() {
        let db = orders_db();
        // UPDATE orders SET id = 2 WHERE id = 1: new image has id = 2, the
        // old-row identity still has id = 1.
        let line = r#"{"action":"U","schema":"public","table":"orders","columns":[{"name":"id","type":"integer","value":2},{"name":"amount","type":"integer","value":100},{"name":"status","type":"text","value":"new"}],"identity":[{"name":"id","type":"integer","value":1}]}"#;
        let ps = patchset_ops(&db, line);
        let ops = ps.ops();
        assert_eq!(
            update_pk(&ops),
            vec![WireValue::Integer(2)],
            "the emitted WHERE targets the new key, so the old key is lost"
        );
    }

    /// The changeset emit path records the old and new value of every
    /// changed column, so a primary-key-changing UPDATE carries both the
    /// old key (for the WHERE) and the new key (for the SET). A patchset
    /// emit cannot express this.
    #[test]
    fn wal2json_changeset_update_captures_old_and_new() {
        let db = orders_db();
        // A pk-changing UPDATE under REPLICA IDENTITY FULL: the new image
        // (`columns`) carries id 2, the old-row image (`identity`) carries
        // id 1.
        let line = r#"{"action":"U","schema":"public","table":"orders","columns":[{"name":"id","type":"integer","value":2},{"name":"amount","type":"integer","value":100},{"name":"status","type":"text","value":"new"}],"identity":[{"name":"id","type":"integer","value":1},{"name":"amount","type":"integer","value":100},{"name":"status","type":"text","value":"new"}]}"#;
        let msg = parse_v2(line).unwrap();

        let bytes = wal2json_changeset(&db, core::slice::from_ref(&msg)).unwrap();
        let ParsedDiffSet::Changeset(diff) = ParsedDiffSet::parse(&bytes).unwrap() else {
            panic!("expected a changeset (marker T)");
        };
        let ops: Vec<_> = diff.iter().collect();
        assert_eq!(ops.len(), 1);
        let ChangesetOp::Update { values, .. } = &ops[0] else {
            panic!("expected an update op, got {:?}", ops[0]);
        };
        // The id column carries the old key (1) and the new key (2), so a
        // changeset apply relocates the row.
        assert_eq!(
            values[0],
            (Some(WireValue::Integer(1)), Some(WireValue::Integer(2)))
        );
    }

    fn orders_uuid_db() -> ParserDB {
        ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id UUID PRIMARY KEY, quantity BIGINT);",
        )
        .unwrap()
    }

    fn only_insert_values(bytes: &[u8]) -> Vec<WireValue<String, Vec<u8>>> {
        let ParsedDiffSet::Patchset(diff) = parse_patchset(bytes) else {
            unreachable!("parse_patchset asserted the Patchset variant");
        };
        let ops: Vec<_> = diff.iter().collect();
        assert_eq!(ops.len(), 1);
        let PatchsetOp::Insert { values, .. } = &ops[0] else {
            panic!("expected an insert op, got {:?}", ops[0]);
        };
        values.to_vec()
    }

    #[test]
    fn pgbinary_adapter_decodes_uuid_to_16_byte_blob() {
        // The whole motivation, pinned at the subql seam: the PgBinary
        // defaults route WireType::Uuid to the 16-byte-blob decoder.
        let uuid_bytes: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let adapter = pgbinary_adapter();
        let value = adapter
            .decode(PgBinaryColumn {
                column_name: "id",
                wire_type: WireType::Uuid,
                raw: Some(&uuid_bytes),
            })
            .unwrap();
        assert_eq!(value, WireValue::Blob(uuid_bytes.to_vec()));
    }

    #[test]
    fn pgbinary_patchset_round_trips_uuid_and_bigint_row() {
        let db = orders_uuid_db();
        let uuid_bytes: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let qty = 42i64.to_be_bytes();
        let rows = vec![vec![Some(uuid_bytes.as_slice()), Some(qty.as_slice())]];
        let bytes = pgbinary_patchset::<crate::backend::Postgres, _>(
            &db,
            "orders",
            &["id", "quantity"],
            &rows,
        )
        .unwrap();
        assert_eq!(
            only_insert_values(&bytes),
            vec![WireValue::Blob(uuid_bytes.to_vec()), WireValue::Integer(42),]
        );
    }

    /// The subql-level guard against the snapshot and CDC encoders drifting:
    /// the same logical uuid lands as the same 16-byte blob whether it
    /// arrives as raw binary (snapshot) or as pgoutput text (CDC).
    #[cfg(feature = "pgoutput-emit")]
    #[test]
    fn pgbinary_and_pgoutput_agree_on_uuid_blob() {
        use sqlite_diff_rs::pg_walstream::{ColumnValue, EventType, Lsn, RowData};

        let db = orders_uuid_db();
        let uuid_bytes: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let uuid_str = "00010203-0405-0607-0809-0a0b0c0d0e0f";

        // Snapshot path: raw 16 binary bytes.
        let qty = 7i64.to_be_bytes();
        let rows = vec![vec![Some(uuid_bytes.as_slice()), Some(qty.as_slice())]];
        let binary_bytes = pgbinary_patchset::<crate::backend::Postgres, _>(
            &db,
            "orders",
            &["id", "quantity"],
            &rows,
        )
        .unwrap();

        // CDC path: the same uuid as pgoutput text.
        let ev = ChangeEvent {
            event_type: EventType::Insert {
                schema: "public".into(),
                table: "orders".into(),
                relation_oid: 1,
                data: RowData::from_pairs(vec![
                    ("id", ColumnValue::text(uuid_str)),
                    ("quantity", ColumnValue::text("7")),
                ]),
            },
            lsn: Lsn::new(1),
            metadata: None,
            xid: None,
        };
        let cdc_bytes = pgoutput_patchset(&db, core::slice::from_ref(&ev)).unwrap();

        assert_eq!(
            only_insert_values(&binary_bytes),
            only_insert_values(&cdc_bytes),
        );
        assert_eq!(
            only_insert_values(&binary_bytes)[0],
            WireValue::Blob(uuid_bytes.to_vec())
        );
    }

    /// A `timestamptz` lands as the same canonical UTC text whether it
    /// arrives as binary microseconds (snapshot) or as pgoutput text in the
    /// server's session time zone (CDC).
    #[cfg(feature = "pgoutput-emit")]
    #[test]
    fn pgbinary_and_pgoutput_agree_on_timestamptz_text() {
        use sqlite_diff_rs::pg_walstream::{ColumnValue, EventType, Lsn, RowData};

        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id UUID PRIMARY KEY, created_at TIMESTAMPTZ NOT NULL);",
        )
        .unwrap();
        let uuid_bytes: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let uuid_str = "00010203-0405-0607-0809-0a0b0c0d0e0f";

        let pg_epoch = chrono::NaiveDate::from_ymd_opt(2000, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc();
        let instant = chrono::NaiveDate::from_ymd_opt(2026, 9, 23)
            .unwrap()
            .and_hms_micro_opt(13, 42, 7, 957_522)
            .unwrap()
            .and_utc();
        let micros = (instant - pg_epoch)
            .num_microseconds()
            .unwrap()
            .to_be_bytes();
        let rows = vec![vec![Some(uuid_bytes.as_slice()), Some(micros.as_slice())]];
        let binary_bytes = pgbinary_patchset::<crate::backend::Postgres, _>(
            &db,
            "orders",
            &["id", "created_at"],
            &rows,
        )
        .unwrap();

        let ev = ChangeEvent {
            event_type: EventType::Insert {
                schema: "public".into(),
                table: "orders".into(),
                relation_oid: 1,
                data: RowData::from_pairs(vec![
                    ("id", ColumnValue::text(uuid_str)),
                    (
                        "created_at",
                        ColumnValue::text("2026-09-23 15:42:07.957522+02"),
                    ),
                ]),
            },
            lsn: Lsn::new(1),
            metadata: None,
            xid: None,
        };
        let cdc_bytes = pgoutput_patchset(&db, core::slice::from_ref(&ev)).unwrap();

        assert_eq!(binary_bytes, cdc_bytes);
        assert_eq!(
            only_insert_values(&binary_bytes)[1],
            WireValue::Text("2026-09-23 13:42:07.957522+00:00".into())
        );
    }

    /// A decimal with a declared scale travels as minor units at that
    /// scale, `NUMERIC(10)` at scale 0, and a bare `NUMERIC`, which holds
    /// any scale, keeps its decimal text. So does a negative scale, which no
    /// count of minor units can express.
    #[test]
    fn only_a_declared_scale_makes_a_decimal_travel_as_minor_units() {
        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE amounts (id INT PRIMARY KEY, amount NUMERIC(10,2), \
             whole NUMERIC(10), bare NUMERIC, hundreds NUMERIC(5,-2));",
        )
        .unwrap();
        let catalog = WireCatalog::from_database(&db).expect("catalog resolves");
        let table = catalog.get(None, "amounts").expect("amounts resolves");
        assert_eq!(table.column_type(1), WireType::ScaledDecimal { scale: 2 });
        assert_eq!(table.column_type(2), WireType::ScaledDecimal { scale: 0 });
        assert_eq!(table.column_type(3), WireType::Decimal);
        assert_eq!(table.column_type(4), WireType::Decimal);
    }

    /// The replica pg2sqlite generates holds a `NUMERIC(10,2)` column as an
    /// `INTEGER` of minor units, so the snapshot and both CDC sources write
    /// `12.50` as `1250`, and write it identically.
    #[cfg(feature = "pgoutput-emit")]
    #[test]
    fn a_scaled_numeric_travels_as_minor_units_on_every_path() {
        use sqlite_diff_rs::pg_walstream::{ColumnValue, EventType, Lsn, RowData};

        // An amount as PostgreSQL binary `numeric` (ndigits, weight, sign,
        // dscale, then base-10000 digits) and as its text output, `None` for
        // NULL.
        type Amount = Option<(&'static [u8], &'static str)>;

        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE amounts (id INT PRIMARY KEY, amount NUMERIC(10,2));",
        )
        .unwrap();
        let amounts: [(i32, Amount); 5] = [
            (
                1,
                Some((&[0, 1, 0xff, 0xff, 0, 0, 0, 2, 0x05, 0x78], "0.14")),
            ),
            (
                2,
                Some((&[0, 2, 0, 0, 0, 0, 0, 2, 0, 12, 0x13, 0x88], "12.50")),
            ),
            (3, Some((&[0, 1, 0, 0, 0, 0, 0, 2, 0, 7], "7.00"))),
            (
                4,
                Some((&[0, 2, 0, 0, 0x40, 0, 0, 2, 0, 3, 0x13, 0x88], "-3.50")),
            ),
            (5, None),
        ];

        let ids: Vec<[u8; 4]> = amounts.iter().map(|(id, _)| id.to_be_bytes()).collect();
        let rows: Vec<Vec<Option<&[u8]>>> = amounts
            .iter()
            .zip(&ids)
            .map(|((_, amount), id)| vec![Some(id.as_slice()), amount.map(|(raw, _)| raw)])
            .collect();
        let snapshot = pgbinary_patchset::<crate::backend::Postgres, _>(
            &db,
            "amounts",
            &["id", "amount"],
            &rows,
        )
        .unwrap();

        let events: Vec<ChangeEvent> = amounts
            .iter()
            .map(|(id, amount)| ChangeEvent {
                event_type: EventType::Insert {
                    schema: "public".into(),
                    table: "amounts".into(),
                    relation_oid: 1,
                    data: RowData::from_pairs(vec![
                        ("id", ColumnValue::text(&id.to_string())),
                        (
                            "amount",
                            amount.map_or(ColumnValue::Null, |(_, text)| ColumnValue::text(text)),
                        ),
                    ]),
                },
                lsn: Lsn::new(1),
                metadata: None,
                xid: None,
            })
            .collect();
        let pgoutput = pgoutput_patchset(&db, &events).unwrap();

        let messages: Vec<_> = amounts
            .iter()
            .map(|(id, amount)| {
                let value = amount.map_or("null", |(_, text)| text);
                parse_v2(&alloc::format!(
                    r#"{{"action":"I","schema":"public","table":"amounts","columns":[{{"name":"id","type":"integer","value":{id}}},{{"name":"amount","type":"numeric(10,2)","value":{value}}}]}}"#
                ))
                .unwrap()
            })
            .collect();
        let wal2json = wal2json_patchset(&db, &messages).unwrap();

        assert_eq!(snapshot, pgoutput, "the snapshot and pgoutput agree");
        assert_eq!(snapshot, wal2json, "the snapshot and wal2json agree");
        let table = catalog_helpers::simple_table(
            &db,
            catalog_helpers::table_id::<crate::backend::Postgres, _>(&db, "amounts").unwrap(),
        )
        .unwrap();
        let expected = [
            (1, WireValue::Integer(14)),
            (2, WireValue::Integer(1250)),
            (3, WireValue::Integer(700)),
            (4, WireValue::Integer(-350)),
            (5, WireValue::Null),
        ]
        .into_iter()
        .fold(
            PatchSet::<SimpleTable, String, Vec<u8>>::new(),
            |builder, (id, amount)| {
                builder.insert(
                    Insert::from(table.clone())
                        .set(0, id)
                        .unwrap()
                        .set(1, amount)
                        .unwrap(),
                )
            },
        );
        assert_eq!(snapshot, expected.build());
    }

    const ARRAY_VECTOR_INTERVAL_DDL: &str = "CREATE EXTENSION vector; \
        CREATE TYPE mood AS ENUM ('happy', 'sad'); \
        CREATE TABLE things (id INT PRIMARY KEY, tags INT[], grid INT[][], names TEXT[], \
        moods mood[], prices NUMERIC(10,2)[], emb vector(3), loose vector, half halfvec(3), \
        span INTERVAL, blobs BYTEA[], ids UUID[], embs vector(3)[]);";

    /// Each column travels as what a pg2sqlite replica stores for it. An
    /// array is JSON text of its innermost element, an enum element as its
    /// label, so `INT[][]` is an `Int` array too. `vector(n)` is a float32
    /// blob, and an interval is PostgreSQL's own text. An array of a scaled
    /// `NUMERIC`, of a BLOB-stored `bytea` or `uuid` or of vectors, and a
    /// `halfvec`, have no replica form, so none gets a wire type and each
    /// keeps the text fallback.
    #[test]
    fn arrays_vectors_and_intervals_travel_as_their_replica_storage() {
        let db = ParserDB::parse::<PostgreSqlDialect>(ARRAY_VECTOR_INTERVAL_DDL).unwrap();
        let catalog = WireCatalog::from_database(&db).expect("catalog resolves");
        let table = catalog.get(None, "things").expect("things resolves");
        let types: Vec<WireType> = (1..13).map(|column| table.column_type(column)).collect();
        assert_eq!(
            types,
            vec![
                WireType::Array {
                    element: ArrayElement::Int
                },
                WireType::Array {
                    element: ArrayElement::Int
                },
                WireType::Array {
                    element: ArrayElement::Text
                },
                WireType::Array {
                    element: ArrayElement::Text
                },
                WireType::Text,
                WireType::Vector {
                    dimensions: Some(3)
                },
                WireType::Vector { dimensions: None },
                WireType::Text,
                WireType::Interval,
                WireType::Text,
                WireType::Text,
                WireType::Text,
            ]
        );
    }

    /// Every element family a replica array holds travels as the element
    /// the matching scalar column would, so an element decodes exactly as
    /// that column does.
    #[test]
    fn every_scalar_element_family_has_its_array_element() {
        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE things (id INT PRIMARY KEY, a BOOLEAN[], b DOUBLE PRECISION[], \
             c NUMERIC[], d DATE[], e TIME[], f TIMESTAMP[], g TIMESTAMPTZ[], h JSON[], \
             i JSONB[], j INTERVAL[]);",
        )
        .unwrap();
        let catalog = WireCatalog::from_database(&db).expect("catalog resolves");
        let table = catalog.get(None, "things").expect("things resolves");
        let elements: Vec<WireType> = (1..11).map(|column| table.column_type(column)).collect();
        let expected: Vec<WireType> = [
            ArrayElement::Bool,
            ArrayElement::Real,
            ArrayElement::Decimal,
            ArrayElement::Date,
            ArrayElement::Time,
            ArrayElement::Timestamp,
            ArrayElement::TimestampTz,
            ArrayElement::Json,
            ArrayElement::Jsonb,
            ArrayElement::Interval,
        ]
        .into_iter()
        .map(|element| WireType::Array { element })
        .collect();
        assert_eq!(elements, expected);
    }

    /// Without `CREATE EXTENSION vector` a type named `vector` is the
    /// schema's own, unknown type, and keeps the text fallback.
    #[test]
    fn a_vector_needs_its_extension() {
        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE things (id INT PRIMARY KEY, emb vector(3));",
        )
        .unwrap();
        let catalog = WireCatalog::from_database(&db).expect("catalog resolves");
        let table = catalog.get(None, "things").expect("things resolves");
        assert_eq!(table.column_type(1), WireType::Text);
    }

    /// The snapshot and both CDC sources write an array, a vector and an
    /// interval identically, as JSON array text, a little-endian float32 blob
    /// and PostgreSQL's `postgres`-style interval text.
    #[cfg(feature = "pgoutput-emit")]
    #[test]
    fn arrays_vectors_and_intervals_agree_on_every_path() {
        use sqlite_diff_rs::pg_walstream::{ColumnValue, EventType, Lsn, RowData};

        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE EXTENSION vector; \
             CREATE TABLE things (id INT PRIMARY KEY, tags INT[], emb vector(3), span INTERVAL);",
        )
        .unwrap();

        let id = 1_i32.to_be_bytes();
        // `array_send` of `{1,2}`: ndim, has-null flag, element OID 23,
        // then the dimension's length and lower bound, then each element.
        let tags: Vec<u8> = [1_i32, 0, 23, 2, 1, 4, 1, 4, 2]
            .iter()
            .flat_map(|word| word.to_be_bytes())
            .collect();
        // pgvector's `vector_send` of `[1,2,3]`: dimension and an unused
        // word as `u16`, then each element as a big-endian `f32`.
        let emb: Vec<u8> = [3_u16, 0]
            .iter()
            .flat_map(|word| word.to_be_bytes())
            .chain([1.0_f32, 2.0, 3.0].iter().flat_map(|x| x.to_be_bytes()))
            .collect();
        // `interval_send` of 15 minutes: microseconds, days, months.
        let span: Vec<u8> = 900_000_000_i64
            .to_be_bytes()
            .into_iter()
            .chain(0_i32.to_be_bytes())
            .chain(0_i32.to_be_bytes())
            .collect();
        let rows = vec![vec![
            Some(id.as_slice()),
            Some(tags.as_slice()),
            Some(emb.as_slice()),
            Some(span.as_slice()),
        ]];
        let snapshot = pgbinary_patchset::<crate::backend::Postgres, _>(
            &db,
            "things",
            &["id", "tags", "emb", "span"],
            &rows,
        )
        .unwrap();

        let event = ChangeEvent {
            event_type: EventType::Insert {
                schema: "public".into(),
                table: "things".into(),
                relation_oid: 1,
                data: RowData::from_pairs(vec![
                    ("id", ColumnValue::text("1")),
                    ("tags", ColumnValue::text("{1,2}")),
                    ("emb", ColumnValue::text("[1,2,3]")),
                    ("span", ColumnValue::text("00:15:00")),
                ]),
            },
            lsn: Lsn::new(1),
            metadata: None,
            xid: None,
        };
        let pgoutput = pgoutput_patchset(&db, core::slice::from_ref(&event)).unwrap();
        let line = r#"{"action":"I","schema":"public","table":"things","columns":[{"name":"id","type":"integer","value":1},{"name":"tags","type":"integer[]","value":"{1,2}"},{"name":"emb","type":"vector","value":"[1,2,3]"},{"name":"span","type":"interval","value":"00:15:00"}]}"#;
        let wal2json = wal2json_patchset(&db, &[parse_v2(line).unwrap()]).unwrap();

        assert_eq!(snapshot, pgoutput, "the snapshot and pgoutput agree");
        assert_eq!(snapshot, wal2json, "the snapshot and wal2json agree");
        let blob: Vec<u8> = [1.0_f32, 2.0, 3.0]
            .iter()
            .flat_map(|x| x.to_le_bytes())
            .collect();
        assert_eq!(
            only_insert_values(&snapshot),
            vec![
                WireValue::Integer(1),
                WireValue::Text("[1,2]".into()),
                WireValue::Blob(blob),
                WireValue::Text("00:15:00".into()),
            ]
        );
    }

    #[test]
    fn pgbinary_null_cell_emits_null() {
        let db = orders_uuid_db();
        let uuid_bytes: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let rows = vec![vec![Some(uuid_bytes.as_slice()), None]];
        let bytes = pgbinary_patchset::<crate::backend::Postgres, _>(
            &db,
            "orders",
            &["id", "quantity"],
            &rows,
        )
        .unwrap();
        assert_eq!(
            only_insert_values(&bytes),
            vec![WireValue::Blob(uuid_bytes.to_vec()), WireValue::Null]
        );
    }

    #[test]
    fn pgbinary_absent_column_stored_as_null() {
        let db = orders_uuid_db();
        let uuid_bytes: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        // The result projects only `id`; `quantity` is absent, so the
        // catalog stores it as NULL.
        let rows = vec![vec![Some(uuid_bytes.as_slice())]];
        let bytes = pgbinary_patchset::<crate::backend::Postgres, _>(&db, "orders", &["id"], &rows)
            .unwrap();
        assert_eq!(
            only_insert_values(&bytes),
            vec![WireValue::Blob(uuid_bytes.to_vec()), WireValue::Null]
        );
    }

    #[test]
    fn pgbinary_row_width_mismatch_is_missing_columns() {
        let db = orders_uuid_db();
        let uuid_bytes: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        // Two column names but a one-cell row.
        let rows = vec![vec![Some(uuid_bytes.as_slice())]];
        let err = pgbinary_patchset::<crate::backend::Postgres, _>(
            &db,
            "orders",
            &["id", "quantity"],
            &rows,
        )
        .unwrap_err();
        assert!(matches!(err, ConversionError::MissingColumns));
    }

    #[test]
    fn pgbinary_unknown_table_is_an_error() {
        let db = orders_uuid_db();
        let uuid_bytes: [u8; 16] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let rows = vec![vec![Some(uuid_bytes.as_slice())]];
        let err = pgbinary_patchset::<crate::backend::Postgres, _>(&db, "absent", &["id"], &rows)
            .unwrap_err();
        assert!(matches!(err, ConversionError::TableNotFound(name) if name == "absent"));
    }

    /// Two schemas declaring the same bare table name must resolve by the
    /// event's source schema, never by which one the catalog enumerated last.
    #[test]
    fn wal2json_resolves_duplicate_table_names_by_schema() {
        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE SCHEMA a; CREATE SCHEMA b; \
             CREATE TABLE a.items (id INT PRIMARY KEY, left_text TEXT); \
             CREATE TABLE b.items (id INT PRIMARY KEY, right_num INT);",
        )
        .unwrap();
        let line = r#"{"action":"I","schema":"a","table":"items","columns":[{"name":"id","type":"integer","value":1},{"name":"left_text","type":"text","value":"x"}]}"#;
        let ps = patchset_ops(&db, line);
        let ops = ps.ops();
        assert_eq!(ops.len(), 1);
        let PatchsetOp::Insert { values, .. } = &ops[0] else {
            panic!("expected an insert op, got {:?}", ops[0]);
        };
        assert_eq!(
            values.to_vec(),
            vec![WireValue::Integer(1), WireValue::Text(String::from("x"))]
        );
        // A source schema neither table carries must refuse, not guess.
        let unknown = r#"{"action":"I","schema":"c","table":"items","columns":[{"name":"id","type":"integer","value":3}]}"#;
        let err =
            wal2json_patchset(&db, core::slice::from_ref(&parse_v2(unknown).unwrap())).unwrap_err();
        assert!(matches!(err, ConversionError::TableNotFound(_)));
    }

    /// Same contract on the pgoutput vehicle.
    #[cfg(feature = "pgoutput-emit")]
    #[test]
    fn pgoutput_resolves_duplicate_table_names_by_schema() {
        use sqlite_diff_rs::pg_walstream::{ColumnValue, EventType, Lsn, RowData};

        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE SCHEMA a; CREATE SCHEMA b; \
             CREATE TABLE a.items (id INT PRIMARY KEY, left_text TEXT); \
             CREATE TABLE b.items (id INT PRIMARY KEY, right_num INT);",
        )
        .unwrap();
        let ev = ChangeEvent {
            event_type: EventType::Insert {
                schema: "a".into(),
                table: "items".into(),
                relation_oid: 1,
                data: RowData::from_pairs(vec![
                    ("id", ColumnValue::text("1")),
                    ("left_text", ColumnValue::text("x")),
                ]),
            },
            lsn: Lsn::new(1),
            metadata: None,
            xid: None,
        };
        let bytes = pgoutput_patchset(&db, core::slice::from_ref(&ev)).unwrap();
        let ParsedDiffSet::Patchset(diff) = parse_patchset(&bytes) else {
            unreachable!("parse_patchset asserted the Patchset variant");
        };
        let ops: Vec<_> = diff.iter().collect();
        assert_eq!(ops.len(), 1);
        let PatchsetOp::Insert { values, .. } = &ops[0] else {
            panic!("expected an insert op, got {:?}", ops[0]);
        };
        assert_eq!(
            values.to_vec(),
            vec![WireValue::Integer(1), WireValue::Text(String::from("x"))]
        );
    }

    /// A Maxwell event qualifies by MySQL database name, which the PG-parsed
    /// catalog does not know. A bare name that is unique across the catalog
    /// must still resolve.
    #[test]
    fn maxwell_database_qualifier_resolves_unqualified_catalog_table() {
        let db = orders_db();
        let msg = maxwell_cdc::parse(
            r#"{"database":"testdb","table":"orders","type":"insert","data":{"id":1,"amount":100,"status":"new"}}"#,
        )
        .unwrap();
        let bytes = maxwell_patchset(&db, core::slice::from_ref(&msg)).unwrap();
        let ParsedDiffSet::Patchset(diff) = parse_patchset(&bytes) else {
            unreachable!("parse_patchset asserted the Patchset variant");
        };
        assert_eq!(diff.iter().count(), 1);
    }

    /// An event naming a column the catalog's table lacks folds into a patch
    /// holding only the catalog's columns. The fold is strict about columns
    /// its schema lacks, so subql projects the event onto its own catalog
    /// before handing it over, the policy for a server built from an older
    /// schema whose database picked up a column afterwards.
    ///
    /// The common event, one whose row names only columns the catalog knows,
    /// allocates nothing on this path. The check compares the row's own
    /// column names against the catalog's table, and a row is rebuilt only
    /// when a column is genuinely unknown, which a schema change makes rare
    /// in a stream.
    #[cfg(feature = "pgoutput-emit")]
    #[test]
    fn pgoutput_fold_projects_an_event_onto_the_catalogs_columns() {
        use sqlite_diff_rs::pg_walstream::{ColumnValue, EventType, Lsn, ReplicaIdentity, RowData};

        // The catalog was built before the server added `status`.
        let db = ParserDB::parse::<PostgreSqlDialect>(
            "CREATE TABLE orders (id INT PRIMARY KEY, amount INT);",
        )
        .unwrap();
        let insert = ChangeEvent {
            event_type: EventType::Insert {
                schema: "public".into(),
                table: "orders".into(),
                relation_oid: 1,
                data: RowData::from_pairs(vec![
                    ("id", ColumnValue::text("1")),
                    ("amount", ColumnValue::text("100")),
                    ("status", ColumnValue::text("new")),
                ]),
            },
            lsn: Lsn::new(1),
            metadata: None,
            xid: None,
        };
        let update = ChangeEvent {
            event_type: EventType::Update {
                schema: "public".into(),
                table: "orders".into(),
                relation_oid: 1,
                // A distinct key, so the fold reports both events rather
                // than the coalesced final state of one key.
                old_data: Some(RowData::from_pairs(vec![
                    ("id", ColumnValue::text("2")),
                    ("amount", ColumnValue::text("100")),
                    ("status", ColumnValue::text("new")),
                ])),
                new_data: RowData::from_pairs(vec![
                    ("id", ColumnValue::text("2")),
                    ("amount", ColumnValue::text("200")),
                    ("status", ColumnValue::text("paid")),
                ]),
                replica_identity: ReplicaIdentity::Full,
                key_columns: alloc::vec!["id".into()],
            },
            lsn: Lsn::new(2),
            metadata: None,
            xid: None,
        };

        let bytes = pgoutput_patchset(&db, &[insert, update]).unwrap();
        let parsed = parse_patchset(&bytes);
        let ParsedDiffSet::Patchset(diff) = &parsed else {
            unreachable!("parse_patchset asserted the Patchset variant");
        };
        let ops: Vec<RowOp<'_>> = diff.iter().collect();
        assert_eq!(ops.len(), 2, "one op per row event: {ops:?}");
        let PatchsetOp::Insert { values, .. } = &ops[0] else {
            panic!("expected an insert op, got {:?}", ops[0]);
        };
        assert_eq!(
            values.to_vec(),
            vec![WireValue::Integer(1), WireValue::Integer(100)],
            "the patch holds only the catalog's columns, in catalog order"
        );
        let PatchsetOp::Update { pk, .. } = &ops[1] else {
            panic!("expected an update op, got {:?}", ops[1]);
        };
        assert_eq!(pk.to_vec(), vec![WireValue::Integer(2)]);
    }
}
