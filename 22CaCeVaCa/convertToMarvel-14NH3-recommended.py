import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

df = pd.read_csv("22CaCeVaCaRecommendedWithoutHeader.txt", delim_whitespace=True)

# Select 14NH3
df["C.1"] == df["C.1"].astype(int)
df = df[df["C.1"] == 1]
df = df.drop("C.1", axis=1)

# Uncertainty
print(df["C.3"].max())
def fixMissingUncertainty(row):
    if row["C.3"] < 1e-10:
        columns = row.keys()
        i = -1
        while i >= -30:
            row[columns[i]] = row[columns[i - 1]]
            i -= 1
        row["C.3"] = 0.003 # Uncertainty if from one transition
    return row

df = df.parallel_apply(lambda x:fixMissingUncertainty(x), axis=1, result_type="expand")

df["Uncertainty"] = df["C.3"]
columns = ["C.2", "C.3", "Uncertainty"] + [f"C.{i}" for i in range(7, 13)] + ["C.15", "C.16", "C.13", "C.18", "C.19"]
columns += [f"C.{i}" for i in range(20, 26)] + ["C.28", "C.29", "C.26", "C.31", "C.32"]
df = df[columns]


# Obtain assigned lines
df["Assigned"] = df.notna().all(axis=1)
df = df[df["Assigned"] == True]
df = df.drop("Assigned", axis=1)
print(len(df))

# 14NH3
# df["C.14"] = df[df["C.14"].astype(int) == 1]
# df = df.drop("C.14", axis=1)
# print(len(df))
columns = df.columns.tolist()
i = 3
while i < len(columns):
    df[columns[i]] = df[columns[i]].astype(int)
    i += 1
    

# print(df.head(15).to_string(index=False))

inversionMapping = {
    0: "s",
    1: "a"
}

df["C.13"] = df["C.13"].map(inversionMapping)
df["C.26"] = df["C.26"].map(inversionMapping)

symmetryMapping = {
    1: "A1'",
    2: "A2'",
    3: "E'",
    4: "A1\"",
    5: "A2\"",
    6: "E\""
}

df["C.18"] = df["C.18"].map(symmetryMapping)
df["C.31"] = df["C.31"].map(symmetryMapping)

df = df.reset_index()
df["Source"] = pd.DataFrame({"Source": [f"22CaCeVaCa.{i + 1}" for i in range(len(df))]})
df = df.drop("index", axis=1)
print(df.head(15).to_string(index=False))
df = df.to_string(index=False)
marvelFile = "AssignedRecommended22CaCeVaCaMarvel.txt"
with open(marvelFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(df)
