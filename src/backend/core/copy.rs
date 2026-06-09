use ahash::AHashMap;
use ordered_float::NotNan;
use sqlparser::ast::{CopyOption, Ident};

use crate::backend::core::column::{ColumnID, DataBaseDataType};
use crate::backend::core::row::{DataBaseDataEntry, RowData, RowID};
use crate::backend::core::table::Table;
use crate::backend::core::transaction::Transaction;
use crate::backend::errors::DataBaseErrors;

#[derive(Debug, Clone)]
pub struct CopyOptions {
    pub delimiter: char,
    pub null_string: String,
    pub header: bool,
    pub binary: bool,
}

impl Default for CopyOptions {
    fn default() -> Self {
        Self {
            delimiter: '\t',
            null_string: "\\N".to_string(),
            header: false,
            binary: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CopyColumnSpec {
    pub name: String,
    pub column_id: ColumnID,
    pub data_type: DataBaseDataType,
}

pub fn parse_copy_options(options: &[CopyOption]) -> Result<CopyOptions, DataBaseErrors> {
    let mut parsed = CopyOptions::default();
    for option in options {
        match option {
            CopyOption::Format(name) => {
                if name.value.eq_ignore_ascii_case("binary") {
                    parsed.binary = true;
                } else if !name.value.eq_ignore_ascii_case("text") && !name.value.eq_ignore_ascii_case("csv") {
                    return Err(DataBaseErrors::QueryError(format!(
                        "Unsupported COPY format '{}'",
                        name.value
                    )));
                }
            }
            CopyOption::Delimiter(delimiter) => parsed.delimiter = *delimiter,
            CopyOption::Null(null) => parsed.null_string = null.clone(),
            CopyOption::Header(header) => parsed.header = *header,
            CopyOption::Freeze(_)
            | CopyOption::Quote(_)
            | CopyOption::Escape(_)
            | CopyOption::ForceQuote(_)
            | CopyOption::ForceNotNull(_)
            | CopyOption::ForceNull(_)
            | CopyOption::Encoding(_) => {}
        }
    }
    Ok(parsed)
}

pub fn resolve_copy_columns(
    table: &Table,
    transaction: &Transaction,
    requested_columns: &[Ident],
    normalize_identifier: fn(&str) -> String,
) -> Result<Vec<CopyColumnSpec>, DataBaseErrors> {
    let column_names: Vec<String> = if !requested_columns.is_empty() {
        requested_columns
            .iter()
            .map(|ident| normalize_identifier(&ident.value))
            .collect()
    } else {
        Vec::new()
    };

    table
        .get_copy_column_specs(transaction, &column_names)
        .map(|specs| {
            specs
                .into_iter()
                .map(|(name, column_id, data_type)| CopyColumnSpec {
                    name,
                    column_id,
                    data_type,
                })
                .collect()
        })
}

pub fn append_copy_data(buffer: &mut Vec<u8>, chunk: &[u8]) {
    buffer.extend_from_slice(chunk);
}

pub fn parse_copy_rows(
    buffer: &[u8],
    columns: &[CopyColumnSpec],
    options: &CopyOptions,
) -> Result<Vec<AHashMap<ColumnID, DataBaseDataEntry>>, DataBaseErrors> {
    if options.binary {
        return Err(DataBaseErrors::QueryError(
            "COPY binary format is not supported".into(),
        ));
    }

    let text = std::str::from_utf8(buffer).map_err(|err| {
        DataBaseErrors::QueryError(format!("COPY data must be valid UTF-8 text: {err}"))
    })?;

    let mut rows = Vec::new();
    let mut skip_header = options.header;
    for line in text.lines() {
        if line.is_empty() {
            continue;
        }
        if skip_header {
            skip_header = false;
            continue;
        }

        let fields = split_copy_fields(line, options.delimiter);
        if fields.len() != columns.len() {
            return Err(DataBaseErrors::QueryError(format!(
                "COPY row has {} fields but {} columns were expected",
                fields.len(),
                columns.len()
            )));
        }

        let mut row_data = AHashMap::new();
        for (field, column) in fields.iter().zip(columns.iter()) {
            let value = parse_copy_value(field, &column.data_type, &options.null_string)?;
            row_data.insert(column.column_id, value);
        }
        rows.push(row_data);
    }

    Ok(rows)
}

pub fn encode_copy_payload(
    rows: &[(RowID, RowData)],
    columns: &[CopyColumnSpec],
    options: &CopyOptions,
) -> Result<Vec<u8>, DataBaseErrors> {
    if options.binary {
        return Err(DataBaseErrors::QueryError(
            "COPY binary format is not supported".into(),
        ));
    }

    let mut payload = Vec::new();
    if options.header {
        let header = columns
            .iter()
            .map(|column| escape_copy_field(&column.name, options.delimiter))
            .collect::<Vec<_>>()
            .join(&options.delimiter.to_string());
        payload.extend_from_slice(header.as_bytes());
        payload.push(b'\n');
    }

    for (_, row) in rows {
        let line = columns
            .iter()
            .map(|column| {
                let value = row
                    .get(&column.column_id)
                    .cloned()
                    .unwrap_or(DataBaseDataEntry::Null);
                format_copy_value(&value, &options.null_string, options.delimiter)
            })
            .collect::<Vec<_>>()
            .join(&options.delimiter.to_string());
        payload.extend_from_slice(line.as_bytes());
        payload.push(b'\n');
    }

    Ok(payload)
}

fn split_copy_fields(line: &str, delimiter: char) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut chars = line.chars().peekable();

    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('b') => current.push('\x08'),
                Some('f') => current.push('\x0c'),
                Some('n') => current.push('\n'),
                Some('r') => current.push('\r'),
                Some('t') => current.push('\t'),
                Some('v') => current.push('\x0b'),
                Some(digit @ '0'..='7') => {
                    let mut octal = String::from(digit);
                    for _ in 0..2 {
                        if let Some(&next @ '0'..='7') = chars.peek() {
                            octal.push(next);
                            chars.next();
                        }
                    }
                    if let Ok(byte) = u8::from_str_radix(&octal, 8) {
                        current.push(byte as char);
                    }
                }
                Some(other) => current.push(other),
                None => current.push('\\'),
            }
        } else if ch == delimiter {
            fields.push(current);
            current = String::new();
        } else {
            current.push(ch);
        }
    }

    fields.push(current);
    fields
}

fn escape_copy_field(value: &str, delimiter: char) -> String {
    let mut escaped = String::new();
    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            c if c == delimiter => {
                escaped.push('\\');
                escaped.push(c);
            }
            c => escaped.push(c),
        }
    }
    escaped
}

fn format_copy_value(
    value: &DataBaseDataEntry,
    null_string: &str,
    delimiter: char,
) -> String {
    let rendered = match value {
        DataBaseDataEntry::Null => return null_string.to_string(),
        DataBaseDataEntry::Boolean(flag) => {
            if *flag {
                "t".to_string()
            } else {
                "f".to_string()
            }
        }
        DataBaseDataEntry::IntegerU8(v) => v.to_string(),
        DataBaseDataEntry::IntegerU16(v) => v.to_string(),
        DataBaseDataEntry::IntegerU32(v) => v.to_string(),
        DataBaseDataEntry::IntegerU64(v) => v.to_string(),
        DataBaseDataEntry::IntegerU128(v) => v.to_string(),
        DataBaseDataEntry::IntegerI8(v) => v.to_string(),
        DataBaseDataEntry::IntegerI16(v) => v.to_string(),
        DataBaseDataEntry::IntegerI32(v) => v.to_string(),
        DataBaseDataEntry::IntegerI64(v) => v.to_string(),
        DataBaseDataEntry::IntegerI128(v) => v.to_string(),
        DataBaseDataEntry::FloatF32(v) => v.into_inner().to_string(),
        DataBaseDataEntry::FloatF64(v) => v.into_inner().to_string(),
        DataBaseDataEntry::String(v) => v.clone(),
        DataBaseDataEntry::Bytes(v) => format!("\\\\x{}", encode_hex(v)),
        DataBaseDataEntry::Timestamp(v) => v.to_string(),
    };
    escape_copy_field(&rendered, delimiter)
}

fn parse_copy_value(
    text: &str,
    data_type: &DataBaseDataType,
    null_string: &str,
) -> Result<DataBaseDataEntry, DataBaseErrors> {
    if text == null_string {
        return Ok(DataBaseDataEntry::Null);
    }

    match data_type {
        DataBaseDataType::Boolean => match text.to_ascii_lowercase().as_str() {
            "t" | "true" | "yes" | "1" => Ok(DataBaseDataEntry::Boolean(true)),
            "f" | "false" | "no" | "0" => Ok(DataBaseDataEntry::Boolean(false)),
            _ => Err(DataBaseErrors::QueryError(format!(
                "Invalid boolean value '{text}' in COPY data"
            ))),
        },
        DataBaseDataType::IntegerU8 => text
            .parse::<u8>()
            .map(DataBaseDataEntry::IntegerU8)
            .map_err(|err| DataBaseErrors::QueryError(format!("Invalid integer '{text}': {err}"))),
        DataBaseDataType::IntegerU16 => text
            .parse::<u16>()
            .map(DataBaseDataEntry::IntegerU16)
            .map_err(|err| DataBaseErrors::QueryError(format!("Invalid integer '{text}': {err}"))),
        DataBaseDataType::IntegerU32 => text
            .parse::<u32>()
            .map(DataBaseDataEntry::IntegerU32)
            .map_err(|err| DataBaseErrors::QueryError(format!("Invalid integer '{text}': {err}"))),
        DataBaseDataType::IntegerU64 => text
            .parse::<u64>()
            .map(DataBaseDataEntry::IntegerU64)
            .map_err(|err| DataBaseErrors::QueryError(format!("Invalid integer '{text}': {err}"))),
        DataBaseDataType::IntegerU128 => text
            .parse::<u128>()
            .map(DataBaseDataEntry::IntegerU128)
            .map_err(|err| DataBaseErrors::QueryError(format!("Invalid integer '{text}': {err}"))),
        DataBaseDataType::IntegerI8 => text
            .parse::<i8>()
            .map(DataBaseDataEntry::IntegerI8)
            .map_err(|err| DataBaseErrors::QueryError(format!("Invalid integer '{text}': {err}"))),
        DataBaseDataType::IntegerI16 => text
            .parse::<i16>()
            .map(DataBaseDataEntry::IntegerI16)
            .map_err(|err| DataBaseErrors::QueryError(format!("Invalid integer '{text}': {err}"))),
        DataBaseDataType::IntegerI32 => text
            .parse::<i32>()
            .map(DataBaseDataEntry::IntegerI32)
            .map_err(|err| DataBaseErrors::QueryError(format!("Invalid integer '{text}': {err}"))),
        DataBaseDataType::IntegerI64 => text
            .parse::<i64>()
            .map(DataBaseDataEntry::IntegerI64)
            .map_err(|err| DataBaseErrors::QueryError(format!("Invalid integer '{text}': {err}"))),
        DataBaseDataType::IntegerI128 => text
            .parse::<i128>()
            .map(DataBaseDataEntry::IntegerI128)
            .map_err(|err| DataBaseErrors::QueryError(format!("Invalid integer '{text}': {err}"))),
        DataBaseDataType::FloatF32 => {
            let value = text.parse::<f32>().map_err(|err| {
                DataBaseErrors::QueryError(format!("Invalid float '{text}': {err}"))
            })?;
            Ok(DataBaseDataEntry::FloatF32(
                NotNan::new(value).map_err(|_| {
                    DataBaseErrors::QueryError("COPY float value must not be NaN".into())
                })?,
            ))
        }
        DataBaseDataType::FloatF64 => {
            let value = text.parse::<f64>().map_err(|err| {
                DataBaseErrors::QueryError(format!("Invalid float '{text}': {err}"))
            })?;
            Ok(DataBaseDataEntry::FloatF64(
                NotNan::new(value).map_err(|_| {
                    DataBaseErrors::QueryError("COPY float value must not be NaN".into())
                })?,
            ))
        }
        DataBaseDataType::String => Ok(DataBaseDataEntry::String(text.to_string())),
        DataBaseDataType::Bytes => {
            if let Some(hex) = text.strip_prefix("\\x") {
                let bytes = decode_hex(hex).map_err(|err| {
                    DataBaseErrors::QueryError(format!("Invalid bytea value '{text}': {err}"))
                })?;
                Ok(DataBaseDataEntry::Bytes(bytes))
            } else {
                Ok(DataBaseDataEntry::Bytes(text.as_bytes().to_vec()))
            }
        }
        DataBaseDataType::Null => Ok(DataBaseDataEntry::Null),
        DataBaseDataType::Timestamp => text
            .parse::<i64>()
            .map(DataBaseDataEntry::Timestamp)
            .or_else(|_| {
                Ok(DataBaseDataEntry::Timestamp(
                    crate::backend::core::plan::dml::current_timestamp_micros(),
                ))
            }),
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::PostgreSqlDialect;
    use sqlparser::parser::Parser;

    use super::*;

    #[test]
    fn split_copy_fields_handles_delimiter_and_escaped_tabs() {
        let fields = split_copy_fields("a\tb\tc", '\t');
        assert_eq!(fields, vec!["a", "b", "c"]);

        let escaped = split_copy_fields(r"a\tb\tc", '\t');
        assert_eq!(escaped, vec!["a\tb\tc"]);
    }

    #[test]
    fn copy_from_stdin_parses_with_trailing_semicolon() {
        let dialect = PostgreSqlDialect {};
        let sql = "COPY copy_test FROM STDIN;";
        let statements = Parser::parse_sql(&dialect, sql).expect("copy should parse");
        assert_eq!(statements.len(), 1);
    }

    #[test]
    fn parse_copy_rows_reads_typed_values() {
        let columns = vec![
            CopyColumnSpec {
                name: "id".to_string(),
                column_id: 0,
                data_type: DataBaseDataType::IntegerI64,
            },
            CopyColumnSpec {
                name: "name".to_string(),
                column_id: 1,
                data_type: DataBaseDataType::String,
            },
        ];
        let options = CopyOptions::default();
        let rows = parse_copy_rows(b"1\talice\n2\tbob\n", &columns, &options).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get(&0), Some(&DataBaseDataEntry::IntegerI64(1)));
        assert_eq!(
            rows[0].get(&1),
            Some(&DataBaseDataEntry::String("alice".to_string()))
        );
    }
}

fn decode_hex(text: &str) -> Result<Vec<u8>, String> {
    if !text.len().is_multiple_of(2) {
        return Err("hex input must have an even number of characters".into());
    }
    (0..text.len())
        .step_by(2)
        .map(|index| {
            u8::from_str_radix(&text[index..index + 2], 16)
                .map_err(|err| err.to_string())
        })
        .collect()
}
