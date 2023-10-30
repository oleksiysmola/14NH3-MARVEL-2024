import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

df = pd.read_csv("22HuSuToWithoutHeader.txt", delim_whitespace=True)
df["Uncertainty"] = 0.001
df["Uncertainty2"] = 0.001
def readVibrationalBand(row):
    for i in range(4):
        row[f"v{i + 1}'"] = row["Band"][i]
    if row["l'"]%3 == 0:
        if row["l'"] >= 0:
            if row["inv'"] == 0:
                row["GammaVib'"] = 1
            else:
                row["GammaVib'"] = 4
        else:
            if row["inv'"] == 0:
                row["GammaVib'"] = 2
            else:
                row["GammaVib'"] = 5
    else:
        if row["inv'"] == 0:
            row["GammaVib'"] = 3
        else:
            row["GammaVib'"] = 6
    # Same for lower state
    if row["l\""]%3 == 0:
        if row["l\""] >= 0:
            if row["inv\""] == 0:
                row["GammaVib\""] = 1
            else:
                row["GammaVib\""] = 4
        else:
            if row["inv\""] == 0:
                row["GammaVib\""] = 2
            else:
                row["GammaVib\""] = 5
    else:
        if row["inv\""] == 0:
            row["GammaVib\""] = 3
        else:
            row["GammaVib\""] = 6
    return row

df = df.parallel_apply(lambda x:readVibrationalBand(x), result_type="expand", axis=1)
df["v1\""] = 0
df["v3\""] = 0
df["v4\""] = 0


inversionMapping = {
    0: "s",
    1: "a"
}

df["inv'"] = df["inv'"].map(inversionMapping)
df["inv\""] = df["inv\""].map(inversionMapping)

symmetryMapping = {
    1: "A1'",
    2: "A2'",
    3: "E'",
    4: "A1\"",
    5: "A2\"",
    6: "E\""
}


statesFileColumns = ["i", "E", "g", "J", "weight", "p", "Gamma", "Nb", "n1", "n2", "n3", "n4", "l3", "l4", "inversion", "J'", "K'", "pRot", "v1", "v2", "v3", "v4", "v5", "v6", "GammaVib", "Calc"]
states = pd.read_csv("../14N-1H3__CoYuTe.states", delim_whitespace=True, names=statesFileColumns)
states = states[states["E"] < 8000]
states = states[states["g"] > 0]
states = states[states["J"] < 15]
print(states.head(5).to_string(index=False))

def findMatchingStates(row, states):
    matchingUpperStates = states[states["J"] == row["J'"]]
    matchingLowerStates = states[states["J"] == row["J\""]]
    matchingUpperStates = matchingUpperStates[matchingUpperStates["K'"] == row["K'"]]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["K'"] == row["K\""]]
    # matchingUpperStates = matchingUpperStates[matchingUpperStates["GammaVib"] == row["GammaVib'"]]
    # matchingLowerStates = matchingLowerStates[matchingLowerStates["GammaVib"] == row["GammaVib\""]]
    if row["E'(UCL)"] > 0:
        matchingUpperStates["Obs-Calc"] = abs(matchingUpperStates["E"] - row["E'(UCL)"])
    else:
        matchingUpperStates["Obs-Calc"] = abs(matchingUpperStates["E"] - row["E'(JA)"])
    matchingLowerStates["Obs-Calc"] = abs(matchingLowerStates["E"] - row["E\""])
    matchingUpperStates = matchingUpperStates.sort_values(by="Obs-Calc")
    matchingLowerStates = matchingLowerStates.sort_values(by="Obs-Calc")
    matchingUpperState = matchingUpperStates.head(1).squeeze()
    matchingLowerState = matchingLowerStates.head(1).squeeze()
    row["L3'"] = matchingUpperState["l3"]
    row["L4'"] = matchingUpperState["l4"]
    row["Gamma'"] = matchingUpperState["Gamma"]
    row["Nb'"] = matchingUpperState["Nb"]
    row["L3\""] = matchingLowerState["l3"]
    row["L4\""] = matchingLowerState["l4"]
    row["Gamma\""] = matchingLowerState["Gamma"]
    row["Nb\""] = matchingLowerState["Nb"]
    return row
        
df = df.parallel_apply(lambda x:findMatchingStates(x, states), axis=1, result_type="expand")
marvelColumns = ["Wavenumber", "Uncertainty", "Uncertainty2"] + [f"v{i}'" for i in range(1,5)] + ["L3'"] + ["L4'"]
marvelColumns += ["J'", "K'", "inv'", "Gamma'", "Nb'"] + [f"v{i}\"" for i in range(1,5)] + ["L3\""] + ["L4\""]
marvelColumns += ["J\"", "K\"", "inv\"", "Gamma\"", "Nb\""]
df = df[marvelColumns]

df["Gamma'"] = df["Gamma'"].map(symmetryMapping)
df["Gamma\""] = df["Gamma\""].map(symmetryMapping)

df = df.reset_index()
df["Source"] = pd.DataFrame({"Source": [f"22HuSuTo.{i + 1}" for i in range(len(df))]})
df = df.drop("index", axis=1)
print(df.head(15).to_string(index=False))
df = df.to_string(index=False)
marvelFile = "22HuSuToMarvel.txt"
with open(marvelFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(df)
