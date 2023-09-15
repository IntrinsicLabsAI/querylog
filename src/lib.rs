// Include a special wrapper row type that we use to translate back and forth with the DataRow
pub mod catalog;
pub mod macros;

use std::sync::Arc;

use futures_util::stream;
use pgwire::{
    api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response},
    error::{PgWireError, PgWireResult},
    messages::data::DataRow,
};
use postgres_types::Type;

/// Internal field type that we can easily cast to internally. We have a set of helper macros that allow us to cast into this type
/// from convenient builtins like String and the integral types
pub enum FieldValue {
    String(String),
    Int4(i32),
}

impl From<String> for FieldValue {
    fn from(value: String) -> Self {
        FieldValue::String(value.clone())
    }
}

impl From<&str> for FieldValue {
    fn from(value: &str) -> Self {
        FieldValue::String(value.to_string())
    }
}

// Use a macro to impl all of the relevant signed integral types
macro_rules! impl_from_for_numeric {
    ($t:ty) => {
        impl From<$t> for FieldValue {
            fn from(value: $t) -> Self {
                FieldValue::Int4(value as i32)
            }
        }
    };
}
impl_from_for_numeric!(i8);
impl_from_for_numeric!(i16);
impl_from_for_numeric!(i32);

/// Simple row concept that we have nice DSL syntax to build.
pub struct SimpleRow {
    schema: Arc<Vec<FieldInfo>>,
    fields: Vec<FieldValue>,
}

impl SimpleRow {
    pub fn new(schema: Arc<Vec<FieldInfo>>, fields: Vec<FieldValue>) -> Self {
        SimpleRow { schema, fields }
    }
}

/// Implementation of TryFrom to convert SimpleRows into DataRows.
impl TryFrom<SimpleRow> for DataRow {
    type Error = PgWireError;

    fn try_from(value: SimpleRow) -> Result<Self, Self::Error> {
        let mut encoder = DataRowEncoder::new(value.schema);

        for field in value.fields {
            match field {
                FieldValue::Int4(ref i4) => encoder.encode_field(i4)?,
                FieldValue::String(ref string) => encoder.encode_field(string)?,
            };
        }

        encoder.finish()
    }
}

#[inline]
pub fn make_query_response<'a>(schema: Arc<Vec<FieldInfo>>, rows: &[DataRow]) -> Response<'a> {
    Response::Query(QueryResponse::new(
        schema,
        stream::iter(
            rows.iter()
                .map(|row| Ok(row.clone()))
                .collect::<Vec<PgWireResult<DataRow>>>(),
        ),
    ))
}

#[inline]
pub fn text_field(name: &str) -> FieldInfo {
    FieldInfo::new(
        name.to_string(),
        None,
        None,
        Type::VARCHAR,
        FieldFormat::Text,
    )
}

#[inline]
pub fn numeric_field(name: &str) -> FieldInfo {
    FieldInfo::new(name.to_string(), None, None, Type::INT4, FieldFormat::Text)
}

#[cfg(test)]
mod tests {
    use crate::macros::row_vec;

    #[test]
    fn test_basic() {
        _ = row_vec![10i32, "Andrew Duffy".to_string()];
    }
}
