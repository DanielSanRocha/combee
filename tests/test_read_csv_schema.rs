use combee::read_csv_schema;

#[test]
fn test_read_csv_schema_basic() {
    let columns = read_csv_schema(String::from("tests/fixtures/basic.csv")).unwrap();
    assert_eq!(
        columns,
        [String::from("name"), String::from("age")].to_vec()
    );
}

#[test]
fn test_read_csv_schema_groupby() {
    let columns = read_csv_schema(String::from("tests/fixtures/groupby.csv")).unwrap();
    assert_eq!(
        columns,
        [String::from("index"), String::from("value")].to_vec()
    );
}
