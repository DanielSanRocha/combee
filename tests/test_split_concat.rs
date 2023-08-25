use combee::dataframe::{DataFrame, concat};

#[test]
fn test_split() {
    let df = DataFrame::new(vec![32,13,4,5,89,32,42,13]);

    let (df1, df2) = df.split(0.3);

    assert_eq!(df1.len(), 6);
    assert_eq!(df2.len(), 2);

    let new_df = concat(&[df1,df2]);

    assert_eq!(new_df.len(), 8);
}