import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

class mapSymmetry(dict):
    def __missing__(self, key):
        symmetry = {
             1: "A1'",
             2: "A2'",
             3: "E'",
             4: "A1\"",
             5: "A2\"",
             6: "E'"
        }[int(str(key)[0])]
        return symmetry

df = pd.read_csv("21CaCeBeCaWithoutHeader.txt", delim_whitespace=True)

df["Assigned"] = df.notna().all(axis=1)
df["Uncertainty"] = 0.003
df["Uncertainty2"] = 0.003

unassigned = df[df["Assigned"] == False] 
unassigned = unassigned.to_string(index=False)
unassignedFile = "Unassigned21CaCeBeCa.txt"
with open(unassignedFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(unassigned)

df =  df[df["Assigned"] == True] 

symmetryMap = mapSymmetry({})

df["SYM’"] = df["SYM’"].map(symmetryMap)
df["SYM\""] = df["SYM\""].map(symmetryMap)
df["J’"] = df["J’"].astype(int)
df["J\""] = df["J\""].astype(int)
df["K’"] = df["K’"].astype(int)
df["K\""] = df["K\""].astype(int)
df["Nbblock’"] = df["Nbblock’"].astype(int)
df["Nbblock\""] = df["Nbblock\""].astype(int)

inversionMapping = {
    0: "s",
    1: "a"
}

df["Inv’"] = df["Inv’"].astype(int).map(inversionMapping)
df["Inv\""] = df["Inv\""].astype(int).map(inversionMapping)


def exctractVibrationalQuantumNumbers(vibrationalMapping):
    vibrationalMapping = str(int(vibrationalMapping))
    vibrationalQuantumNumbers = []
    for i in range(6 - len(vibrationalMapping)):
        vibrationalQuantumNumbers += [0]
    for i in range(len(vibrationalMapping)):
        vibrationalQuantumNumbers += [int(vibrationalMapping[i])]
    return vibrationalQuantumNumbers

def obtainVibrationalQuantumNumbers(row):
    upperVibrationalQuantumNumbers = exctractVibrationalQuantumNumbers(row["Vib’"])
    lowerVibrationalQuantumNumbers = exctractVibrationalQuantumNumbers(row["Vib\""])
    for i in range(len(upperVibrationalQuantumNumbers)):
        row[f"v{i + 1}’"] = upperVibrationalQuantumNumbers[i]
        row[f"v{i + 1}\""] = lowerVibrationalQuantumNumbers[i]
    return row
    
df = df.parallel_apply(lambda x:obtainVibrationalQuantumNumbers(x), axis=1, result_type="expand")
df = df.reset_index()
df["Source"] = pd.DataFrame({"Source": [f"21CaCeBeCa.{i + 1}" for i in range(len(df))]})
df = df[["Transition", "Uncertainty", "Uncertainty2", 
         "v1’",
         "v2’",
         "v3’",
         "v4’",
         "v5’",
         "v6’",
         "J’",
         "K’",
         "Inv’",
         "SYM’",
         "Nbblock’",
         "v1\"",
         "v2\"",
         "v3\"",
         "v4\"",
         "v5\"",
         "v6\"",
         "J\"",
         "K\"",
         "Inv\"",
         "SYM\"",
         "Nbblock\"",
         "Source"]]

print(df.head(5).to_string(index=False))

df = df.to_string(index=False)
marvelFile = "Assigned21CaCeBeCaMarvel.txt"
with open(marvelFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(df)
   
