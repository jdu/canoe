//! Converts an inferred Arrow schema into an Avro schema document (`.avsc`
//! JSON) describing a dataset's rows — written alongside a sink's Parquet
//! output so a reader can see a dataset's shape without opening a
//! Parquet file. Best-effort: covers the scalar types
//! `arrow::json::reader::infer_json_schema_from_iterator` actually
//! produces from typical JSON event payloads; anything it doesn't
//! recognize (nested lists/structs, in particular) falls back to Avro
//! `"string"` rather than failing the write — this is descriptive
//! metadata alongside the authoritative Parquet data, not a schema
//! contract enforced anywhere, so a slightly-off entry here shouldn't
//! block a flush.

use arrow::datatypes::{DataType, Schema};
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// `column_metadata`: a sink's compiled `SinkStep.column_metadata` — for
/// any field whose name has an entry, its map is embedded verbatim as a
/// `canoe.metadata` custom property, giving downstream readers (a
/// person, `canoe-tui`, an external catalog tool) labels/descriptions/
/// sensitivity tags right next to the field they describe, without a
/// separate sidecar file. Fields with no entry are left exactly as
/// before this parameter existed.
pub fn arrow_schema_to_avsc(
    schema: &Schema,
    record_name: &str,
    column_metadata: &BTreeMap<String, BTreeMap<String, Value>>,
) -> Value {
    let fields: Vec<Value> = schema
        .fields()
        .iter()
        .map(|field| {
            let avro_type = arrow_type_to_avro(field.data_type());
            let field_type = if field.is_nullable() {
                json!(["null", avro_type])
            } else {
                avro_type
            };
            let mut entry = serde_json::Map::new();
            entry.insert("name".to_string(), json!(field.name()));
            entry.insert("type".to_string(), field_type);
            if field.is_nullable() {
                entry.insert("default".to_string(), Value::Null);
            }
            if let Some(meta) = column_metadata.get(field.name()) {
                entry.insert("canoe.metadata".to_string(), json!(meta));
            }
            Value::Object(entry)
        })
        .collect();

    json!({
        "type": "record",
        "name": avro_record_name(record_name),
        "fields": fields,
    })
}

fn arrow_type_to_avro(data_type: &DataType) -> Value {
    match data_type {
        DataType::Null => json!("null"),
        DataType::Boolean => json!("boolean"),
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => {
            json!("int")
        }
        DataType::Int64 | DataType::UInt64 => json!("long"),
        DataType::Float32 => json!("float"),
        DataType::Float64 => json!("double"),
        DataType::Utf8 | DataType::LargeUtf8 => json!("string"),
        DataType::Binary | DataType::LargeBinary => json!("bytes"),
        DataType::Date32 | DataType::Date64 => json!({"type": "int", "logicalType": "date"}),
        DataType::Timestamp(_, _) => json!({"type": "long", "logicalType": "timestamp-millis"}),
        // Nested/list/struct/etc: not produced richly by the batch-JSON
        // inference this sink uses today, and mapping them faithfully
        // needs recursive Avro union/record types. Documented fallback.
        _ => json!("string"),
    }
}

/// Avro record names must match `[A-Za-z_][A-Za-z0-9_]*`; dataset ids are
/// free-form strings (e.g. containing `-`), so sanitize rather than fail.
fn avro_record_name(dataset_id: &str) -> String {
    let mut name: String = dataset_id
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' })
        .collect();
    if name.is_empty() || name.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        name.insert(0, '_');
    }
    name
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::Field;

    #[test]
    fn converts_common_types() {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Float64, true),
        ]);
        let avsc = arrow_schema_to_avsc(&schema, "orders-raw", &BTreeMap::new());
        assert_eq!(avsc["type"], "record");
        assert_eq!(avsc["name"], "orders_raw");
        let fields = avsc["fields"].as_array().unwrap();
        assert_eq!(fields[0]["name"], "id");
        assert_eq!(fields[0]["type"], "long");
        assert_eq!(fields[1]["name"], "name");
        assert_eq!(fields[1]["type"], json!(["null", "string"]));
        assert_eq!(fields[1]["default"], Value::Null);
    }

    #[test]
    fn embeds_canoe_metadata_only_for_matching_fields() {
        let schema = Schema::new(vec![Field::new("email", DataType::Utf8, true), Field::new("id", DataType::Int64, false)]);
        let mut column_metadata = BTreeMap::new();
        let mut email_meta = BTreeMap::new();
        email_meta.insert("sensitivity".to_string(), json!("restricted"));
        email_meta.insert("classification".to_string(), json!(["pii", "contact"]));
        column_metadata.insert("email".to_string(), email_meta);

        let avsc = arrow_schema_to_avsc(&schema, "customers", &column_metadata);
        let fields = avsc["fields"].as_array().unwrap();
        let email_field = fields.iter().find(|f| f["name"] == "email").unwrap();
        assert_eq!(email_field["canoe.metadata"]["sensitivity"], "restricted");
        assert_eq!(email_field["canoe.metadata"]["classification"], json!(["pii", "contact"]));

        let id_field = fields.iter().find(|f| f["name"] == "id").unwrap();
        assert!(id_field.get("canoe.metadata").is_none(), "id has no metadata entry, should be untouched");
    }

    #[test]
    fn sanitizes_record_name() {
        assert_eq!(avro_record_name("orders-raw"), "orders_raw");
        assert_eq!(avro_record_name("123abc"), "_123abc");
    }
}
