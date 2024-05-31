import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)
pd.set_option('display.precision', 10)
def writeDataFrameToFile(dataFrame, fileName, header=True):
    dataFrame = dataFrame.to_string(index=False, header=header)
    with open(fileName, "w+") as fileToWriteTo:
        fileToWriteTo.write(dataFrame)

columnNames = ["nu", "intensity", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "L'", "J'", "K'", "i'", "Gvib'", "Grot'", "Gtot'", 
    "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "L\"", "J\"", "K\"", "i\"", "Gvib\"", "Grot\"", "Gtot\"", "Method"]

transitions = pd.read_csv("16BaYuTeBe.txt", delim_whitespace=True, names=columnNames)
transitions["unc1"] = 0.01
transitions["unc2"] = 0.01
transitions = transitions.dropna()
transitions = transitions[~transitions["nu1'"].str.contains('\*', na=False)]
# transitions df[~df.apply(lambda row: row.astype(str).str.contains('\*', regex=False).any(), axis=1)]
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
        row["CoYuTeBandTag"] = str(matchingUpperState["nu1"]) + "-" + str(matchingUpperState["nu2"])  + "-" + str(matchingUpperState["nu3"])  + "-" + str(matchingUpperState["nu4"])  + "-" + str(matchingUpperState["L3"])  + "-" + str(matchingUpperState["L4"]) +  "-" + str(matchingUpperState["inv"])
        row["BandTag"] = str(row["nu1'"]) + "-" + str(row["nu2'"])  + "-" + str(row["nu3'"])  + "-" + str(row["nu4'"])  + "-" + str(row["L3'"])  + "-" + str(row["L4'"]) +  "-" + str(row["i'"])
        row["BandMatch"] = row["CoYuTeBandTag"] == row["BandTag"]
    else:
        row["Calc'"] = -1000
        row["Nb'"] = -1000
        row["CoYuTeBandTag"] = "-1000"
        row["BandMatch"] = False
    return row

transitions = transitions.parallel_apply(lambda x:findMatchingStates(x, states), result_type="expand", axis=1)
print(len(transitions))
transitions["J'"] = transitions["J'"].astype(int)
transitions["K'"] = transitions["K'"].astype(int)
transitions["i'"] = transitions["i'"].astype(int)
transitions["nu1\""] = transitions["nu1\""].astype(int)
transitions["nu2\""] = transitions["nu2\""].astype(int)
transitions["nu3\""] = transitions["nu3\""].astype(int)
transitions["nu4\""] = transitions["nu4\""].astype(int)
transitions["L3\""] = transitions["L3\""].astype(int)
transitions["L4\""] = transitions["L4\""].astype(int)
transitions["J\""] = transitions["J\""].astype(int)
transitions["K\""] = transitions["K\""].astype(int)
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
transitions["Source"] = [f"16BaYuTeBe.{i + 1}" for i in range(len(transitions))]
print(len(transitions))
transitions = transitions.sort_values(by="E'")
transitions = transitions.groupby(["J'", "Gtot'"])
def assignBlockNumbers(dataFrame):
    blockNumberAssignments = [-1]
    if len(dataFrame) >= 2:
        for i in range(1, len(dataFrame)):
            if dataFrame.iloc[i]["E'"] - dataFrame.iloc[i-1]["E'"] < 0.05:
                blockNumberAssignments += [blockNumberAssignments[i - 1]]
            else:
                blockNumberAssignments += [blockNumberAssignments[i - 1] - 1]
    dataFrame["Nb'"] = blockNumberAssignments
    return dataFrame
transitions = transitions.parallel_apply(lambda x:assignBlockNumbers(x))
print(transitions.head(20).to_string())
transitionsToMarvel = transitions[["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "i'", "Gtot'", "Nb'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "i\"", "Gtot\"", "Nb\"", "Source"]]#[transitions["Method"] == "CD"]
writeDataFrameToFile(transitionsToMarvel, "16BaYuTeBe-MARVEL.txt", header=False)
transitionsComparison = transitions[["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "i'", "Gtot'", "Nb'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "i\"", "Gtot\"", "Nb\"", "E'", "Calc'", "Obs-Calc'", "Source", "Method", "BandMatch"]]
print("\n")
print("Number of transitions with matching bands: ", len(transitionsComparison[transitionsComparison["BandMatch"]]))
writeDataFrameToFile(transitionsComparison, "16BaYuTeBeAgainstStatesFile.txt")
print("\n")
print("Number of CD assignments: ", len(transitions[transitions["Method"] == "CD"]))
print("\n")
print("Number of CD assignments smaller than 1 cm-1 obs-calc: ", len(transitions[transitions["Method"] == "CD"][abs(transitions["Obs-Calc'"]) < 1]))