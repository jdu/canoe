//! Turns a step's ordered declarative ops into executable code: Python
//! source for `engine = "python"` steps, a Spark SQL query for
//! `engine = "spark_sql"` steps. An escape-hatch `python { code = ... }` or
//! `spark_sql { sql = ... }` block, if present, is used verbatim instead —
//! see `ast::RawOp::PythonCode`/`SparkSqlCode`.

use crate::ast::{RawMappingSource, RawOp};
use crate::expr;
use anyhow::{bail, Context};
use serde_json::{Map, Value};
use std::path::Path;

/// `def run(record): ...` implementing every op in order. `record` is a
/// plain dict decoded from the input topic's JSON; ops mutate/replace it
/// and the final value is returned (or `None` to drop the record, from a
/// `filter` op whose predicate didn't match). `base_dir` resolves any
/// `file = ...` escape hatch (or `map_values` mapping file) relative to
/// the `.hcl` source file.
pub fn python(ops: &[RawOp], base_dir: &Path) -> anyhow::Result<String> {
    if let Some(RawOp::PythonCode { code }) = ops.iter().find(|o| matches!(o, RawOp::PythonCode { .. })) {
        if ops.len() > 1 {
            bail!("a step's `python {{ code = ... }}` escape hatch must be its only block");
        }
        return code.resolve(base_dir);
    }

    let mut needs_json_import = false;
    let mut needs_mapping_loader = false;
    let mut preamble = String::new();
    let mut body = String::new();
    let mut map_counter = 0usize;

    for op in ops {
        match op {
            RawOp::Filter { predicate } => {
                let py_pred = expr::to_python(predicate);
                body.push_str(&format!(
                    "    if not eval({py_pred:?}, {{}}, record):\n        return None\n"
                ));
            }
            RawOp::Rename { from, to } => {
                body.push_str(&format!(
                    "    if {from:?} in record:\n        record[{to:?}] = record.pop({from:?})\n"
                ));
            }
            RawOp::Drop { fields } => {
                for f in fields {
                    body.push_str(&format!("    record.pop({f:?}, None)\n"));
                }
            }
            RawOp::Select { fields } => {
                let keys = fields.iter().map(|f| format!("{f:?}")).collect::<Vec<_>>().join(", ");
                body.push_str(&format!(
                    "    record = {{k: record[k] for k in [{keys}] if k in record}}\n"
                ));
            }
            RawOp::Cast { field, to } => {
                let caster = python_caster(to)
                    .with_context(|| format!("cast to type '{to}' is not supported on a python-engine step (supported: string, int, float, bool; use engine=\"spark_sql\" for timestamp/date)"))?;
                body.push_str(&format!(
                    "    if {field:?} in record and record[{field:?}] is not None:\n        record[{field:?}] = {caster}(record[{field:?}])\n"
                ));
            }
            RawOp::WithField { name, expression } => {
                let py_expr = expr::to_python(expression);
                body.push_str(&format!(
                    "    record[{name:?}] = eval({py_expr:?}, {{}}, record)\n"
                ));
            }
            RawOp::MapValues { field, to, mapping, default } => {
                let var = format!("_MAP_{map_counter}");
                map_counter += 1;

                match resolve_mapping_json(mapping, base_dir)? {
                    Some(map) => {
                        needs_json_import = true;
                        let json_text = serde_json::to_string(&Value::Object(map))
                            .expect("a serde_json::Map always serializes");
                        preamble.push_str(&format!("{var} = json.loads({json_text:?})\n"));
                    }
                    None => {
                        // S3 source: not knowable at compile time, so defer
                        // to a runtime fetch the worker image provides —
                        // see workers/python-worker/canoe_worker/mapping.py.
                        needs_mapping_loader = true;
                        let RawMappingSource::S3 { bucket, key } = mapping else {
                            unreachable!("resolve_mapping_json only returns None for an S3 source")
                        };
                        let code_source_json = serde_json::json!({
                            "kind": "s3",
                            "bucket": bucket,
                            "key": key,
                        })
                        .to_string();
                        preamble.push_str(&format!("{var} = load_json_mapping({code_source_json:?})\n"));
                    }
                }

                let target = to.clone().unwrap_or_else(|| field.clone());
                let default_expr = match default {
                    Some(d) => format!("{d:?}"),
                    None => format!("record.get({field:?})"),
                };
                body.push_str(&format!(
                    "    record[{target:?}] = {var}.get(record.get({field:?}), {default_expr})\n"
                ));
            }
            RawOp::PythonCode { .. } | RawOp::SparkSqlCode { .. } | RawOp::SparkDataFrameCode { .. } | RawOp::UvScript { .. } => {
                bail!("escape-hatch code blocks cannot be combined with other ops in the same step");
            }
        }
    }
    body.push_str("    return record\n");

    let mut source = String::new();
    if needs_json_import {
        source.push_str("import json\n");
    }
    if needs_mapping_loader {
        source.push_str("from canoe_worker.mapping import load_json_mapping\n");
    }
    if needs_json_import || needs_mapping_loader {
        source.push('\n');
    }
    source.push_str(&preamble);
    if !preamble.is_empty() {
        source.push('\n');
    }
    source.push_str("def run(record):\n");
    source.push_str(&body);

    Ok(source)
}

fn python_caster(dsl_type: &str) -> Option<&'static str> {
    match dsl_type {
        "string" => Some("str"),
        "int" => Some("int"),
        "float" => Some("float"),
        "bool" => Some("bool"),
        _ => None,
    }
}

/// A single Spark SQL query (a chain of CTEs, one per op, run against the
/// `input` view) implementing every op in order.
///
/// Rename/drop/select are expressed with `SELECT * EXCEPT (...)`, which
/// requires Spark 3.4+ / Databricks SQL's star-exclusion syntax — a
/// documented constraint of this first pass rather than building a full
/// column-algebra planner.
pub fn spark_sql(ops: &[RawOp], base_dir: &Path) -> anyhow::Result<String> {
    if let Some(RawOp::SparkSqlCode { sql }) = ops.iter().find(|o| matches!(o, RawOp::SparkSqlCode { .. })) {
        if ops.len() > 1 {
            bail!("a step's `spark_sql {{ sql = ... }}` escape hatch must be its only block");
        }
        return sql.resolve(base_dir);
    }

    if ops.is_empty() {
        return Ok("SELECT * FROM input".to_string());
    }

    let mut ctes = Vec::new();
    let mut prev = "input".to_string();

    for (i, op) in ops.iter().enumerate() {
        let cte_name = format!("s{i}");
        let select = match op {
            RawOp::Filter { predicate } => format!("SELECT * FROM {prev} WHERE {predicate}"),
            RawOp::Rename { from, to } => {
                format!("SELECT * EXCEPT ({from}), {from} AS {to} FROM {prev}")
            }
            RawOp::Drop { fields } => {
                format!("SELECT * EXCEPT ({}) FROM {prev}", fields.join(", "))
            }
            RawOp::Select { fields } => format!("SELECT {} FROM {prev}", fields.join(", ")),
            RawOp::Cast { field, to } => {
                let sql_type = spark_sql_type(to)
                    .with_context(|| format!("cast to type '{to}' is not a supported Spark SQL type"))?;
                format!("SELECT * EXCEPT ({field}), CAST({field} AS {sql_type}) AS {field} FROM {prev}")
            }
            RawOp::WithField { name, expression } => {
                format!("SELECT *, {expression} AS {name} FROM {prev}")
            }
            RawOp::MapValues { field, to, mapping, default } => {
                let map = resolve_mapping_json(mapping, base_dir)?.with_context(|| {
                    "map_values with an S3 mapping source isn't supported for engine=\"spark_sql\" \
                     (this codegen only produces static SQL text, it can't emit a runtime fetch); \
                     use engine=\"python\", or a local `file`/inline `code` mapping instead"
                })?;
                let target = to.clone().unwrap_or_else(|| field.clone());
                let mut whens = String::new();
                for (k, v) in &map {
                    whens.push_str(&format!(
                        "WHEN CAST({field} AS STRING) = {} THEN {} ",
                        sql_string_literal(k),
                        sql_json_value_literal(v)
                    ));
                }
                let else_clause = match default {
                    Some(d) => sql_string_literal(d),
                    None => field.clone(),
                };
                let case_expr = format!("CASE {whens}ELSE {else_clause} END");
                if &target == field {
                    format!("SELECT * EXCEPT ({field}), {case_expr} AS {field} FROM {prev}")
                } else {
                    format!("SELECT *, {case_expr} AS {target} FROM {prev}")
                }
            }
            RawOp::PythonCode { .. } | RawOp::SparkSqlCode { .. } | RawOp::SparkDataFrameCode { .. } | RawOp::UvScript { .. } => {
                bail!("escape-hatch code blocks cannot be combined with other ops in the same step");
            }
        };
        ctes.push(format!("{cte_name} AS ({select})"));
        prev = cte_name;
    }

    Ok(format!("WITH {}\nSELECT * FROM {prev}", ctes.join(",\n     ")))
}

fn spark_sql_type(dsl_type: &str) -> Option<&'static str> {
    match dsl_type {
        "string" => Some("STRING"),
        "int" => Some("BIGINT"),
        "float" => Some("DOUBLE"),
        "bool" => Some("BOOLEAN"),
        "timestamp" => Some("TIMESTAMP"),
        "date" => Some("DATE"),
        _ => None,
    }
}

fn sql_string_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn sql_json_value_literal(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => sql_string_literal(s),
        other => sql_string_literal(&other.to_string()),
    }
}

/// Resolves a `map_values` mapping source to a parsed JSON object at
/// compile time — `Inline`/`File` always resolve; `S3` returns `None`
/// (only the Python codegen path can honor it, via a runtime fetch).
fn resolve_mapping_json(mapping: &RawMappingSource, base_dir: &Path) -> anyhow::Result<Option<Map<String, Value>>> {
    let text = match mapping {
        RawMappingSource::Inline(text) => text.clone(),
        RawMappingSource::File(rel_path) => {
            let full_path = base_dir.join(rel_path);
            std::fs::read_to_string(&full_path).with_context(|| {
                format!("failed to read mapping file \"{rel_path}\" (resolved to {})", full_path.display())
            })?
        }
        RawMappingSource::S3 { .. } => return Ok(None),
    };
    let value: Value = serde_json::from_str(&text).context("mapping source is not valid JSON")?;
    match value {
        Value::Object(map) => Ok(Some(map)),
        other => bail!("mapping source must be a JSON object (e.g. {{\"US\": \"United States\"}}), got: {other}"),
    }
}

/// Resolves a sink's `metadata = "column-metadata.yaml"` attribute to a
/// per-column metadata map at compile time — same "`file = ...` read once
/// by `canoe-dslc`, embedded in the compiled config" precedent as
/// `resolve_mapping_json`'s `File` case, just YAML instead of JSON and
/// with no `Inline`/`S3` variants (there's exactly one file per sink, no
/// stated need for either). Each column's value is left as an open
/// `serde_json::Value` map — `label`/`description`/`sensitivity`/
/// `classification` are the documented examples, not a fixed schema, so
/// any other key a YAML file declares passes through untouched.
pub fn resolve_column_metadata(
    path: Option<&str>,
    base_dir: &Path,
) -> anyhow::Result<std::collections::BTreeMap<String, std::collections::BTreeMap<String, Value>>> {
    let Some(rel_path) = path else {
        return Ok(std::collections::BTreeMap::new());
    };
    let full_path = base_dir.join(rel_path);
    let text = std::fs::read_to_string(&full_path).with_context(|| {
        format!("failed to read column metadata file \"{rel_path}\" (resolved to {})", full_path.display())
    })?;
    let value: Value = serde_yaml::from_str(&text)
        .with_context(|| format!("column metadata file \"{rel_path}\" is not valid YAML"))?;
    let Value::Object(columns) = value else {
        bail!(
            "column metadata file \"{rel_path}\" must be a YAML mapping of column name -> metadata \
             (e.g. `customer_id:\\n  label: \"Customer ID\"`), got: {value}"
        );
    };
    columns
        .into_iter()
        .map(|(column, meta)| {
            let Value::Object(meta) = meta else {
                bail!(
                    "column metadata file \"{rel_path}\": column \"{column}\"'s value must be a mapping \
                     (e.g. `label: \"...\"`), got: {meta}"
                );
            };
            Ok((column, meta.into_iter().collect()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_python_for_common_ops() {
        let ops = vec![
            RawOp::Filter {
                predicate: "amount > 0".to_string(),
            },
            RawOp::Rename {
                from: "cust_id".to_string(),
                to: "customer_id".to_string(),
            },
            RawOp::WithField {
                name: "flag".to_string(),
                expression: "amount > 100".to_string(),
            },
        ];
        let src = python(&ops, Path::new(".")).unwrap();
        assert!(src.contains("def run(record):"));
        assert!(src.contains("eval(\"amount > 0\""));
        assert!(src.contains("record[\"customer_id\"] = record.pop(\"cust_id\")"));
        assert!(src.contains("return record"));
    }

    #[test]
    fn generates_spark_sql_ctes() {
        let ops = vec![
            RawOp::Filter {
                predicate: "amount > 0".to_string(),
            },
            RawOp::Rename {
                from: "cust_id".to_string(),
                to: "customer_id".to_string(),
            },
        ];
        let sql = spark_sql(&ops, Path::new(".")).unwrap();
        assert!(sql.contains("s0 AS (SELECT * FROM input WHERE amount > 0)"));
        assert!(sql.contains("s1 AS (SELECT * EXCEPT (cust_id), cust_id AS customer_id FROM s0)"));
        assert!(sql.trim_end().ends_with("SELECT * FROM s1"));
    }

    #[test]
    fn generates_python_for_inline_map_values() {
        let ops = vec![RawOp::MapValues {
            field: "region_code".to_string(),
            to: Some("region_name".to_string()),
            mapping: RawMappingSource::Inline(r#"{"US": "United States", "CA": "Canada"}"#.to_string()),
            default: Some("Unknown".to_string()),
        }];
        let src = python(&ops, Path::new(".")).unwrap();
        assert!(src.contains("import json"));
        assert!(src.contains("_MAP_0 = json.loads("));
        assert!(src.contains("United States"));
        assert!(src.contains(
            "record[\"region_name\"] = _MAP_0.get(record.get(\"region_code\"), \"Unknown\")"
        ));
    }

    #[test]
    fn generates_python_for_s3_map_values() {
        let ops = vec![RawOp::MapValues {
            field: "region_code".to_string(),
            to: None,
            mapping: RawMappingSource::S3 {
                bucket: "my-bucket".to_string(),
                key: "maps/region.json".to_string(),
            },
            default: None,
        }];
        let src = python(&ops, Path::new(".")).unwrap();
        assert!(src.contains("from canoe_worker.mapping import load_json_mapping"));
        assert!(src.contains("_MAP_0 = load_json_mapping("));
        assert!(src.contains("my-bucket"));
        // in-place (no `to`) and no default: falls back to the original value
        assert!(src.contains(
            "record[\"region_code\"] = _MAP_0.get(record.get(\"region_code\"), record.get(\"region_code\"))"
        ));
    }

    #[test]
    fn generates_spark_sql_case_when_for_map_values() {
        let ops = vec![RawOp::MapValues {
            field: "region_code".to_string(),
            to: Some("region_name".to_string()),
            mapping: RawMappingSource::Inline(r#"{"US": "United States"}"#.to_string()),
            default: None,
        }];
        let sql = spark_sql(&ops, Path::new(".")).unwrap();
        assert!(sql.contains("WHEN CAST(region_code AS STRING) = 'US' THEN 'United States'"));
        assert!(sql.contains("ELSE region_code END AS region_name"));
    }

    #[test]
    fn spark_sql_rejects_s3_map_values() {
        let ops = vec![RawOp::MapValues {
            field: "region_code".to_string(),
            to: None,
            mapping: RawMappingSource::S3 {
                bucket: "b".to_string(),
                key: "k".to_string(),
            },
            default: None,
        }];
        assert!(spark_sql(&ops, Path::new(".")).is_err());
    }
}
