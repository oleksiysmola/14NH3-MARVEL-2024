import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

df = pd.read_csv("23CaCeVoWithoutHeader.txt", delim_whitespace=True)

# Return only assigned lines
df = df[df["C.28"].notna()]
df["Uncertainty missing"] = df["C.29"].isna()

def shiftColumns(row):
    if row["Uncertainty missing"]:
        i = 29
        while i > 8:
            row[f"C.{i}"] = row[f"C.{i - 1}"]
            i = i - 1
    return row

df = df.parallel_apply(lambda x:shiftColumns(x), result_type="expand", axis=1)
# print(df.head(40).to_string(index=False))
df["C.8"] = df["C.8"].astype(float)
print("Max: ", df[df["C.8"] < 0.9913]["C.8"].max())

def fixMissingUncertainty(row):
    if row["Uncertainty missing"]:
        row["C.8"] = 0.007 # Uncertainty if from one transition
    return row

df = df.parallel_apply(lambda x:fixMissingUncertainty(x), axis=1, result_type="expand")

df["C.16"] = df["C.16"].astype(int)
df["C.23"] = df["C.23"].astype(int)
df["C.16"] = df["C.16"].astype(str)
df["C.23"] = df["C.23"].astype(str)

def obtainQuantumNumbers(row):
    i = 0
    while i < 6 - len(row["C.16"]):
        row[f"v{i + 1}'"] = 0
        i += 1
    for j in range(len(row["C.16"])):
        row[f"v{i + 1}'"] = row["C.16"][j]
        i += 1
    i = 0
    while i < 6 - len(row["C.23"]):
        row[f"v{i + 1}\""] = 0
        i += 1
    for j in range(len(row["C.23"])):
        row[f"v{i + 1}\""] = row["C.23"][j]
        i += 1
    row["Gamma'"] = str(row["C.13"])[0]
    row["Gamma\""] = str(row["C.20"])[0]
    return row

df = df.parallel_apply(lambda x:obtainQuantumNumbers(x), axis=1, result_type="expand")




# def removeNullUncertainties(row):
#     if row["C.3"] < 0.00001:
#         row["C.3"] = 0.00001
#     return row

# df = df.parallel_apply(lambda x:removeNullUncertainties(x), axis=1, result_type="expand")

df["Uncertainty"] = df["C.8"]
columns = ["C.7", "C.8", "Uncertainty"] + [f"v{i}'" for i in range(1, 7)] + ["C.14", "C.15", "C.17", "Gamma'", "C.19"]
columns += [f"v{i}\"" for i in range(1, 7)] + ["C.21", "C.22", "C.24", "Gamma\"", "C.26"]
df = df[columns]


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

df["C.17"] = df["C.17"].map(inversionMapping)
df["C.24"] = df["C.24"].map(inversionMapping)

symmetryMapping = {
    1: "A1'",
    2: "A2'",
    3: "E'",
    4: "A1\"",
    5: "A2\"",
    6: "E\""
}

df["Gamma'"] = df["Gamma'"].map(symmetryMapping)
df["Gamma\""] = df["Gamma\""].map(symmetryMapping)

df = df.reset_index()
df["Source"] = pd.DataFrame({"Source": [f"23CaCeVo.{i + 1}" for i in range(len(df))]})
df = df.drop("index", axis=1)
print(df.head(15).to_string(index=False))
df = df.to_string(index=False)
marvelFile = "Assigned23CaCeVoMarvel.txt"
with open(marvelFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(df)
