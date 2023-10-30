import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

df = pd.read_csv("21CeCaCoWithoutHeader.txt", delim_whitespace=True)


df["Uncertainty"] = df["C4"]
columns = ["C3", "C4", "Uncertainty"] + [f"C{i}" for i in range(14, 20)] + ["C22", "C23", "C20", "C25", "C26"]
columns += [f"C{i}" for i in range(27, 33)] + ["C35", "C36", "C33", "C38", "C39"]
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

df["C20"] = df["C20"].map(inversionMapping)
df["C33"] = df["C33"].map(inversionMapping)

symmetryMapping = {
    1: "A1'",
    2: "A2'",
    3: "E'",
    4: "A1\"",
    5: "A2\"",
    6: "E\""
}

df["C25"] = df["C25"].map(symmetryMapping)
df["C38"] = df["C38"].map(symmetryMapping)

df = df.reset_index()
df["Source"] = pd.DataFrame({"Source": [f"21CeCaCo.{i + 1}" for i in range(len(df))]})
# df = df.drop("index", axis=1)
print(df.head(15).to_string(index=False))
df = df.to_string(index=False)
marvelFile = "Assigned21CeCaCoMarvel.txt"
with open(marvelFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(df)
