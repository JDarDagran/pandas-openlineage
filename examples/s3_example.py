import pandas_openlineage as pd

df = pd.read_csv("s3://air-example-data/regression.csv", usecols=["x000", "x001", "y"])
df.to_csv("/tmp/test.csv")
