def assert_dataframes(df1, df2):
    assert df1.collect() == df2.collect()
