import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

def writeDataFrameToFile(dataFrame, fileName):
    dataFrame = dataFrame.to_string(index=False)
    with open(fileName, "w+") as fileToWriteTo:
        fileToWriteTo.write(dataFrame)

columnNames = ["ErrByte", "nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "i'", "Gvib'", "Grot'", "Gtot'", 
    "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "i\"", "Gvib\"", "Grot\"", "Gtot\"", "Method"]

transitions = pd.read_csv("17BaPoYuTe.txt", delim_whitespace=True, names=columnNames)

print(transitions[["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "i'", "Gvib'", "Grot'", "Gtot'", 
    "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "i\"", "Gvib\"", "Grot\"", "Gtot\""]].head(20).to_string(index=False))
statesFileColumns = ["i", "E", "g", "J", "weight", "p", "Gamma", "Nb", "nu1", "nu2", "nu3", "nu4", "L3", "L4", "inv", "J'", "K'", "pRot", "v1", "v2", "v3", "v4", "v5", "v6", "GammaVib", "Calc"]
states = pd.read_csv("../14N-1H3__CoYuTe.states", delim_whitespace=True, names=statesFileColumns)
states = states[states["E"] < 12000]
states = states[states["g"] > 0]
states = states[states["J"] <= 15] 
def findMatchingStates(row, states):
    matchingLowerStates = states[states["J"] == row["J\""]]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["nu1"] == 0]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["nu2"] == 0]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["nu3"] == 0]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["nu4"] == 0]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["L3"] == 0]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["L4"] == 0]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["K'"] == row["K\""]]
    # matchingLowerStates = matchingLowerStates[matchingLowerStates["Gamma"] == "Gtot\""]
    # matchingLowerStates = matchingLowerStates[matchingLowerStates["pRot"] == "Grot\""]
    # matchingLowerStates = matchingLowerStates[matchingLowerStates["GammaVib"] == "Gvib\""]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["inv"] == row["i\""]]
    matchingLowerState = matchingLowerStates.sort_values(by="E").head(1).squeeze()
    row["E\""] = matchingLowerState["E"]
    row["Nb\""] = matchingLowerState["Nb"]
    row["E'"] = row["E\""] + row["nu"]
    matchingUpperStates = states[states["J"] == row["J'"]]
    matchingUpperStates = matchingUpperStates[matchingUpperStates["Gamma"] == row["Gtot'"]]
    # matchingUpperStates = matchingUpperStates[matchingUpperStates["pRot"] == row["Grot'"]]
    # matchingUpperStates = matchingUpperStates[matchingUpperStates["GammaVib"] == row["Gvib'"]]
    # matchingUpperStates = matchingUpperStates[matchingUpperStates["inv"] == row["i'"]]
    matchingUpperStates["Diff"] = abs(row["E'"] - matchingUpperStates["E"])
    matchingUpperStates = matchingUpperStates.sort_values(by="Diff")
    if len(matchingUpperStates) >= 1:
        matchingUpperState = matchingUpperStates.head(1).squeeze()
        row["Calc'"] = matchingUpperState["E"]
        row["Nb'"] = matchingUpperState["Nb"]
        row["Obs-Calc'"] = row["E'"] - row["Calc'"]
    else:
        row["Calc'"] = -1000
        row["Nb'"] = -1000
    return row

transitions["Source"] = pd.DataFrame({"Source": [f"17BaPoYuTe.{i + 1}" for i in range(len(transitions))]})
transitions = transitions.parallel_apply(lambda x:findMatchingStates(x, states), result_type="expand", axis=1)
transitions = transitions.sort_values(by="E'")
symmetryMap = {
    1: "A1'",
    2: "A2'",
    3: "E'",
    4: "A1\"",
    5: "A2\"",
    6: "E\""
}
transitions["Gtot'"] = transitions["Gtot'"].map(symmetryMap)
transitions["Gtot\""] = transitions["Gtot\""].map(symmetryMap)
inversionMap = {
    0: "s",
    1: "a"
}
transitions["i'"] = transitions["i'"].map(inversionMap)
transitions["i\""] = transitions["i\""].map(inversionMap)
transitionsToMarvel = transitions[["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "i'", "Gtot'", "Nb'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "i\"", "Gtot\"", "Nb\"", "Source"]]
writeDataFrameToFile(transitionsToMarvel, "17BaPoYuTe-MARVEL.txt")
transitionsComparison = transitions[["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "i'", "Gtot'", "Nb'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "i\"", "Gtot\"", "Nb\"", "E'", "Calc'", "Obs-Calc'", "Source", "Method"]]
writeDataFrameToFile(transitionsComparison, "17BaPoYuTeAgainstStatesFile.txt")
print("\n")
print("Number of CD assignments: ", len(transitions[transitions["Method"] == "CD"]))
print("\n")
print("Number of CD assignments smaller than 1 cm-1 obs-calc: ", len(transitions[transitions["Method"] == "CD"][abs(transitions["Obs-Calc'"]) < 1]))