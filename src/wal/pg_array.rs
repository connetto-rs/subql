//! PostgreSQL array cells as the server writes them, read into their
//! elements.
//!
//! An array travels as `array_out`'s text on a text stream and in wal2json,
//! and as `array_send`'s bytes on a binary pgoutput stream. Either way the
//! elements are visited in row-major order, the order `UNNEST` yields them, so
//! a multidimensional value reads as its flattened elements.

use alloc::string::String;
use alloc::vec::Vec;

use super::pg_type::text_to_pg_value_by_kind;
use crate::backend::{decode_cell, ArrayCell, Postgres, ScalarKindOf, Value};
use crate::types::ColumnId;
use crate::ValueError;

/// The array cell `text` carries, each element decoded from its text form as
/// `element`, the column's declared element type.
///
/// # Errors
///
/// [`ValueError::Array`] for text that is not an array, and the error
/// [`decode_cell`] gives for an element that is not a value of `element`.
pub fn text_array_cell(
    column: ColumnId,
    element: ScalarKindOf<Postgres>,
    text: &str,
) -> Result<ArrayCell<Postgres>, ValueError> {
    let mut values = Vec::new();
    text_array_elements(column, text, |item| {
        values.push(match item {
            None => Value::Null,
            Some(item) => decode_cell(column, element, |builtin| {
                text_to_pg_value_by_kind(item, builtin)
            })?,
        });
        Ok(())
    })?;
    Ok(ArrayCell::Elements(values))
}

/// PostgreSQL refuses an array with more dimensions, so a deeper value is not
/// one the server wrote.
const MAX_DIMENSIONS: usize = 6;

/// Visit each element of `text`, PostgreSQL's array output, in row-major
/// order, `None` for a `NULL` element.
///
/// The grammar read is `array_out`'s, an optional `[lower:upper]...=`
/// decoration, braces, `,` between elements, and double quotes around any
/// element that is empty, spells `NULL`, or holds a delimiter, brace, quote,
/// backslash or blank, with a backslash before each quote or backslash
/// inside them. Anything else is not a value the server wrote.
///
/// # Errors
///
/// [`ValueError::Array`] for text that is not such an array, including one
/// whose sub-arrays differ in length or disagree with the decoration, and
/// whatever `visit` returns for an element.
fn text_array_elements(
    column: ColumnId,
    text: &str,
    visit: impl FnMut(Option<&str>) -> Result<(), ValueError>,
) -> Result<(), ValueError> {
    let mut reader = TextArray {
        column,
        text,
        at: 0,
        extents: [None; MAX_DIMENSIONS],
        leaf_depth: None,
        scratch: String::new(),
        visit,
    };
    let decorated = reader.decoration()?;
    reader.body()?;
    if reader.at != text.len() {
        return Err(reader.malformed());
    }
    match decorated {
        Some((extents, dimensions)) => {
            let read = reader.leaf_depth.map_or(0, |leaf| leaf + 1);
            let agrees = read == dimensions
                && extents[..dimensions]
                    .iter()
                    .zip(&reader.extents)
                    .all(|(declared, found)| Some(*declared) == *found);
            if agrees {
                Ok(())
            } else {
                Err(reader.malformed())
            }
        }
        None => Ok(()),
    }
}

struct TextArray<'a, F> {
    column: ColumnId,
    text: &'a str,
    at: usize,
    /// The length every sub-array at each depth has, once one was read.
    extents: [Option<usize>; MAX_DIMENSIONS],
    /// The depth whose sub-arrays hold elements rather than sub-arrays.
    leaf_depth: Option<usize>,
    /// The unescaped text of a quoted element that held a backslash.
    scratch: String,
    visit: F,
}

impl<F> TextArray<'_, F>
where
    F: FnMut(Option<&str>) -> Result<(), ValueError>,
{
    const fn malformed(&self) -> ValueError {
        ValueError::Array {
            column: self.column,
        }
    }

    fn peek(&self) -> Option<u8> {
        self.text.as_bytes().get(self.at).copied()
    }

    fn expect(&mut self, byte: u8) -> Result<(), ValueError> {
        if self.peek() == Some(byte) {
            self.at += 1;
            Ok(())
        } else {
            Err(self.malformed())
        }
    }

    /// The extent of each dimension the `[lower:upper]...=` prefix declares,
    /// and how many it declares, or `None` without one.
    fn decoration(&mut self) -> Result<Option<([usize; MAX_DIMENSIONS], usize)>, ValueError> {
        if self.peek() != Some(b'[') {
            return Ok(None);
        }
        let mut extents = [0; MAX_DIMENSIONS];
        let mut dimensions = 0;
        while self.peek() == Some(b'[') {
            self.at += 1;
            let lower = self.bound(b':')?;
            let upper = self.bound(b']')?;
            let extent = upper
                .checked_sub(lower)
                .and_then(|span| usize::try_from(span).ok())
                .and_then(|span| span.checked_add(1));
            let (Some(slot), Some(extent)) = (extents.get_mut(dimensions), extent) else {
                return Err(self.malformed());
            };
            *slot = extent;
            dimensions += 1;
        }
        self.expect(b'=')?;
        Ok(Some((extents, dimensions)))
    }

    /// One signed bound of a decoration, and the byte that ends it.
    fn bound(&mut self, end: u8) -> Result<i64, ValueError> {
        let rest = &self.text[self.at..];
        let length = rest.find(char::from(end)).ok_or_else(|| self.malformed())?;
        let bound = rest[..length]
            .parse::<i32>()
            .map_err(|_| self.malformed())?;
        self.at += length + 1;
        Ok(i64::from(bound))
    }

    /// The braced value, where only the outermost level may be empty.
    fn body(&mut self) -> Result<(), ValueError> {
        if self.text.as_bytes().get(self.at..self.at + 2) == Some(b"{}") {
            self.at += 2;
            return Ok(());
        }
        self.level(0)
    }

    fn level(&mut self, depth: usize) -> Result<(), ValueError> {
        if depth == MAX_DIMENSIONS {
            return Err(self.malformed());
        }
        self.expect(b'{')?;
        let nested = self.peek() == Some(b'{');
        if !nested {
            if self.leaf_depth.is_some_and(|leaf| leaf != depth) {
                return Err(self.malformed());
            }
            self.leaf_depth = Some(depth);
        }
        let mut length = 0_usize;
        loop {
            if nested {
                self.level(depth + 1)?;
            } else {
                self.element()?;
            }
            length += 1;
            match self.peek() {
                Some(b',') => self.at += 1,
                Some(b'}') => {
                    self.at += 1;
                    break;
                }
                _ => return Err(self.malformed()),
            }
        }
        match self.extents[depth] {
            Some(extent) if extent != length => Err(self.malformed()),
            Some(_) => Ok(()),
            None => {
                self.extents[depth] = Some(length);
                Ok(())
            }
        }
    }

    fn element(&mut self) -> Result<(), ValueError> {
        if self.peek() == Some(b'"') {
            return self.quoted();
        }
        let start = self.at;
        let length = self.text[start..]
            .find(|character: char| {
                matches!(character, ',' | '}' | '{' | '"' | '\\') || character.is_ascii_whitespace()
            })
            .unwrap_or(self.text.len() - start);
        if length == 0 || !matches!(self.text.as_bytes().get(start + length), Some(b',' | b'}')) {
            return Err(self.malformed());
        }
        self.at = start + length;
        let element = &self.text[start..self.at];
        if element.eq_ignore_ascii_case("NULL") {
            (self.visit)(None)
        } else {
            (self.visit)(Some(element))
        }
    }

    fn quoted(&mut self) -> Result<(), ValueError> {
        let start = self.at + 1;
        let mut run = start;
        let mut at = start;
        let mut escaped = false;
        self.scratch.clear();
        loop {
            match self.text.as_bytes().get(at) {
                None => return Err(self.malformed()),
                Some(b'"') => break,
                Some(b'\\') => {
                    let next = self.text[at + 1..]
                        .chars()
                        .next()
                        .ok_or_else(|| self.malformed())?;
                    self.scratch.push_str(&self.text[run..at]);
                    self.scratch.push(next);
                    at += 1 + next.len_utf8();
                    run = at;
                    escaped = true;
                }
                Some(_) => at += 1,
            }
        }
        self.at = at + 1;
        if escaped {
            self.scratch.push_str(&self.text[run..at]);
            (self.visit)(Some(&self.scratch))
        } else {
            (self.visit)(Some(&self.text[start..at]))
        }
    }
}

/// The element type and elements of `bytes`, in PostgreSQL's binary array
/// format of dimension count, null flag, element type OID, a length and
/// lower bound per dimension, then each element as a length (`-1` for
/// `NULL`) and that many bytes.
///
/// # Errors
///
/// [`ValueError::Array`] for a header PostgreSQL would refuse, and, while
/// iterating, for an element that overruns the bytes or bytes left over
/// after the last element.
#[cfg(any(feature = "pg-streaming", feature = "pg-sqlite-emu"))]
pub fn binary_array_elements(
    column: ColumnId,
    bytes: &[u8],
) -> Result<BinaryElements<'_>, ValueError> {
    let malformed = ValueError::Array { column };
    let mut rest = bytes;
    let dimensions = read_i32(&mut rest).ok_or_else(|| malformed.clone())?;
    let flags = read_i32(&mut rest).ok_or_else(|| malformed.clone())?;
    let element_oid = read_i32(&mut rest).ok_or_else(|| malformed.clone())?;
    let dimensions = usize::try_from(dimensions)
        .ok()
        .filter(|dimensions| *dimensions <= MAX_DIMENSIONS)
        .ok_or_else(|| malformed.clone())?;
    if !matches!(flags, 0 | 1) {
        return Err(malformed);
    }
    let mut count = usize::from(dimensions > 0);
    for _ in 0..dimensions {
        let extent = read_i32(&mut rest)
            .and_then(|extent| usize::try_from(extent).ok())
            .ok_or_else(|| malformed.clone())?;
        read_i32(&mut rest).ok_or_else(|| malformed.clone())?;
        count = count.checked_mul(extent).ok_or_else(|| malformed.clone())?;
    }
    Ok(BinaryElements {
        column,
        element_oid: element_oid.cast_unsigned(),
        remaining: count,
        rest,
    })
}

/// The elements of one binary array, in row-major order.
#[cfg(any(feature = "pg-streaming", feature = "pg-sqlite-emu"))]
pub struct BinaryElements<'a> {
    column: ColumnId,
    element_oid: u32,
    remaining: usize,
    rest: &'a [u8],
}

#[cfg(any(feature = "pg-streaming", feature = "pg-sqlite-emu"))]
impl BinaryElements<'_> {
    /// The type the header names for every element.
    pub(crate) const fn element_oid(&self) -> u32 {
        self.element_oid
    }

    /// How many elements the header declares, bounded by the bytes that
    /// could hold them, since each takes at least its four-byte length.
    pub(crate) const fn capacity(&self) -> usize {
        let fit = self.rest.len() / 4;
        if self.remaining < fit {
            self.remaining
        } else {
            fit
        }
    }
}

#[cfg(any(feature = "pg-streaming", feature = "pg-sqlite-emu"))]
impl<'a> Iterator for BinaryElements<'a> {
    type Item = Result<Option<&'a [u8]>, ValueError>;

    fn next(&mut self) -> Option<Self::Item> {
        let malformed = ValueError::Array {
            column: self.column,
        };
        if self.remaining == 0 {
            if self.rest.is_empty() {
                return None;
            }
            self.rest = &[];
            return Some(Err(malformed));
        }
        self.remaining -= 1;
        let Some(length) = read_i32(&mut self.rest) else {
            self.remaining = 0;
            return Some(Err(malformed));
        };
        if length == -1 {
            return Some(Ok(None));
        }
        let Some((element, rest)) = usize::try_from(length)
            .ok()
            .and_then(|length| self.rest.split_at_checked(length))
        else {
            self.remaining = 0;
            self.rest = &[];
            return Some(Err(malformed));
        };
        self.rest = rest;
        Some(Ok(Some(element)))
    }
}

#[cfg(any(feature = "pg-streaming", feature = "pg-sqlite-emu"))]
fn read_i32(bytes: &mut &[u8]) -> Option<i32> {
    let (head, rest) = bytes.split_first_chunk::<4>()?;
    *bytes = rest;
    Some(i32::from_be_bytes(*head))
}

/// `array_send`'s bytes for `elements` of type `element_oid`, laid out in
/// `dimensions`, each an extent and a lower bound.
#[cfg(all(test, any(feature = "pg-streaming", feature = "pg-sqlite-emu")))]
pub fn array_send(
    element_oid: u32,
    dimensions: &[(i32, i32)],
    elements: &[Option<&[u8]>],
) -> Vec<u8> {
    let mut out = Vec::new();
    let depth = i32::try_from(dimensions.len()).expect("a short test array");
    out.extend_from_slice(&depth.to_be_bytes());
    out.extend_from_slice(&i32::from(elements.iter().any(Option::is_none)).to_be_bytes());
    out.extend_from_slice(&element_oid.to_be_bytes());
    for (extent, lower) in dimensions {
        out.extend_from_slice(&extent.to_be_bytes());
        out.extend_from_slice(&lower.to_be_bytes());
    }
    for element in elements {
        match element {
            None => out.extend_from_slice(&(-1_i32).to_be_bytes()),
            Some(bytes) => {
                let size = i32::try_from(bytes.len()).expect("a short test element");
                out.extend_from_slice(&size.to_be_bytes());
                out.extend_from_slice(bytes);
            }
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use alloc::string::{String, ToString};
    use alloc::vec::Vec;

    use super::text_array_elements;
    use crate::ValueError;

    fn elements(text: &str) -> Result<Vec<Option<String>>, ValueError> {
        let mut out = Vec::new();
        text_array_elements(7, text, |element| {
            out.push(element.map(ToString::to_string));
            Ok(())
        })
        .map(|()| out)
    }

    fn text(items: &[Option<&str>]) -> Vec<Option<String>> {
        items
            .iter()
            .map(|item| item.map(ToString::to_string))
            .collect()
    }

    #[test]
    fn plain_and_quoted_elements_read_as_the_server_wrote_them() {
        assert_eq!(
            elements(r#"{alice,"bob smith","",NULL,"NULL","a\"b\\c",é}"#).unwrap(),
            text(&[
                Some("alice"),
                Some("bob smith"),
                Some(""),
                None,
                Some("NULL"),
                Some(r#"a"b\c"#),
                Some("é"),
            ])
        );
    }

    #[test]
    fn an_unquoted_null_is_null_in_any_case() {
        assert_eq!(elements("{null,Null}").unwrap(), text(&[None, None]));
    }

    #[test]
    fn the_empty_array_has_no_elements() {
        assert_eq!(elements("{}").unwrap(), text(&[]));
    }

    #[test]
    fn a_multidimensional_array_flattens_in_row_major_order() {
        assert_eq!(
            elements("{{a,b,c},{d,NULL,f}}").unwrap(),
            text(&[Some("a"), Some("b"), Some("c"), Some("d"), None, Some("f")])
        );
    }

    #[test]
    fn a_decoration_that_agrees_with_the_value_is_read_past() {
        assert_eq!(
            elements("[0:1]={a,b}").unwrap(),
            text(&[Some("a"), Some("b")])
        );
        assert_eq!(
            elements("[-1:0][2:3]={{a,b},{c,d}}").unwrap(),
            text(&[Some("a"), Some("b"), Some("c"), Some("d")])
        );
    }

    #[test]
    fn text_the_server_would_not_write_is_refused() {
        for malformed in [
            "",
            "{",
            "alice",
            "{a,b",
            "{a,,b}",
            "{a,}",
            "{a} ",
            "{ a}",
            "{a b}",
            r#"{a"b}"#,
            r"{a\b}",
            r#"{"a}"#,
            "{{a},b}",
            "{a,{b}}",
            "{{a,b},{c}}",
            "{{a},{{b}}}",
            "{{}}",
            "[0:2]={a,b}",
            "[1:2][1:1]={a,b}",
            "[1:2]{a,b}",
            "[2:1]={}",
            "{{{{{{{a}}}}}}}",
            "{{a}{b}}",
            "[x:2]={a,b}",
            "[1:99999999999]={a}",
            r#"{"a\"#,
        ] {
            assert_eq!(
                elements(malformed),
                Err(ValueError::Array { column: 7 }),
                "{malformed}"
            );
        }
    }

    #[test]
    fn six_dimensions_are_read() {
        assert_eq!(elements("{{{{{{a}}}}}}").unwrap(), text(&[Some("a")]));
    }

    #[test]
    fn an_element_the_visitor_refuses_stops_the_read() {
        let refusal = ValueError::Builtin {
            column: 7,
            kind: crate::backend::ScalarFamily::Int,
        };
        let mut seen = 0;
        let read = text_array_elements(7, "{1,x,3}", |element| {
            seen += 1;
            if element == Some("x") {
                Err(refusal.clone())
            } else {
                Ok(())
            }
        });
        assert_eq!(read, Err(refusal));
        assert_eq!(seen, 2);
    }

    #[cfg(any(feature = "pg-streaming", feature = "pg-sqlite-emu"))]
    mod binary {
        use alloc::vec::Vec;

        use super::super::{array_send, binary_array_elements};
        use crate::ValueError;

        /// The element type OID and the elements' bytes, `None` for `NULL`.
        type Read = (u32, Vec<Option<Vec<u8>>>);

        /// `array_send`'s bytes for text elements laid out in `dimensions`.
        fn array(dimensions: &[(i32, i32)], elements: &[Option<&[u8]>]) -> Vec<u8> {
            array_send(25, dimensions, elements)
        }

        fn read(bytes: &[u8]) -> Result<Read, ValueError> {
            let elements = binary_array_elements(3, bytes)?;
            let oid = elements.element_oid();
            let read = elements
                .map(|element| element.map(|element| element.map(<[u8]>::to_vec)))
                .collect::<Result<_, _>>()?;
            Ok((oid, read))
        }

        #[test]
        fn elements_read_in_row_major_order_with_nulls() {
            let bytes = array(
                &[(2, 1), (2, 0)],
                &[Some(b"a"), None, Some(b""), Some(b"dd")],
            );
            assert_eq!(
                read(&bytes).unwrap(),
                (
                    25,
                    alloc::vec![
                        Some(b"a".to_vec()),
                        None,
                        Some(Vec::new()),
                        Some(b"dd".to_vec())
                    ]
                )
            );
        }

        #[test]
        fn a_zero_dimensional_array_is_empty() {
            assert_eq!(read(&array(&[], &[])).unwrap(), (25, Vec::new()));
        }

        #[test]
        fn bytes_that_disagree_with_the_header_are_refused() {
            let whole = array(&[(2, 1)], &[Some(b"a"), Some(b"b")]);
            let mut extra = whole.clone();
            extra.push(0);
            let mut negative_extent = array(&[(1, 1)], &[Some(b"a")]);
            negative_extent[12..16].copy_from_slice(&(-1_i32).to_be_bytes());
            let mut bad_flags = whole.clone();
            bad_flags[4..8].copy_from_slice(&2_i32.to_be_bytes());
            let mut too_deep = array(&[], &[]);
            too_deep[0..4].copy_from_slice(&7_i32.to_be_bytes());
            let mut cut_length = array(&[(1, 1)], &[]);
            cut_length.extend_from_slice(&[0, 0]);
            for malformed in [
                &whole[..whole.len() - 1],
                &whole[..10],
                extra.as_slice(),
                negative_extent.as_slice(),
                bad_flags.as_slice(),
                too_deep.as_slice(),
                cut_length.as_slice(),
            ] {
                assert_eq!(read(malformed), Err(ValueError::Array { column: 3 }));
            }
        }
    }
}
