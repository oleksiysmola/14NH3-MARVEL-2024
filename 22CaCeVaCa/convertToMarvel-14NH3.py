import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

df = pd.read_csv("22CaCeVaCaWithoutHeader.txt", delim_whitespace=True)

# Uncertainty
df["C.8"] = abs(df["C.8"])
def fixMissingUncertainty(row):
    if row["C.8"] > 1:
        columns = row.keys()
        i = -1
        while i >= -23:
            row[columns[i]] = row[columns[i - 1]]
            i -= 1
        row["C.8"] = 0.02 # Uncertainty if from one transition
    return row

df = df.parallel_apply(lambda x:fixMissingUncertainty(x), axis=1, result_type="expand")
df = df.drop("C.13", axis=1)

columns = ["C.5", "C.8", "C.14"] + [f"C.{i}" for i in range(15, 21)] + ["C.23", "C.24", "C.21", "C.26", "C.27"]
columns += [f"C.{i}" for i in range(28, 34)] + ["C.36", "C.37", "C.34", "C.39", "C.40"]
df = df[columns]


# 14NH3
# df["C.14"] = df[df["C.14"].astype(int) == 1]
# df = df.drop("C.14", axis=1)
df["Assigned"] = df.notna().all(axis=1)
df = df[df["Assigned"] == True]
df = df.drop("Assigned", axis=1)
# print(len(df))
columns = df.columns.tolist()
i = 2
while i < len(columns):
    df[columns[i]] = df[columns[i]].astype(int)
    i += 1
    
df["Uncertainty"] = df["C.8"]
columns = ["C.5", "C.8", "Uncertainty"] + [f"C.{i}" for i in range(15, 21)] + ["C.23", "C.24", "C.21", "C.26", "C.27"]
columns += [f"C.{i}" for i in range(28, 34)] + ["C.36", "C.37", "C.34", "C.39", "C.40"]
df = df[columns]

# print(df.head(15).to_string(index=False))

inversionMapping = {
    0: "s",
    1: "a"
}

df["C.21"] = df["C.21"].map(inversionMapping)
df["C.34"] = df["C.34"].map(inversionMapping)

symmetryMapping = {
    1: "A1'",
    2: "A2'",
    3: "E'",
    4: "A1\"",
    5: "A2\"",
    6: "E\""
}

df["C.26"] = df["C.26"].map(symmetryMapping)
df["C.39"] = df["C.39"].map(symmetryMapping)

df = df.reset_index()
df["Source"] = pd.DataFrame({"Source": [f"22CaCeVaCa.{i + 1}" for i in range(len(df))]})
df = df.drop("index", axis=1)
print(df.head(15).to_string(index=False))
df = df.to_string(index=False)
marvelFile = "Assigned22CaCeVaCaMarvel.txt"
with open(marvelFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(df)
