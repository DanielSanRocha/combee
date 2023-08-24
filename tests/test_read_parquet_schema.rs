use combee::{read_parquet_schema};

#[test]
fn test_read_parquet_schema_basic() {
    let schema = read_parquet_schema("tests/fixtures/basic.parquet".to_string()).unwrap();
    assert_eq!(schema, "Field { name: \"name\", data_type: Utf8, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }, Field { name: \"age\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }".to_string());
}
