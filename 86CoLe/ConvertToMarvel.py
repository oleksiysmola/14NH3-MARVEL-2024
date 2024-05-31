import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

transitionsColumns = ["nu", "unc1", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "inv'", "GammaRot'", "GammaVib'", "Gamma'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "inv\"", "GammaRot\"", "GammaVib\"", "Gamma\"",  "Source"]


transitions = pd.read_csv("86CoLe.txt", delim_whitespace=True, names=transitionsColumns)
transitions = transitions[transitions["nu"] > 0]
print(transitions.head(20).to_string(index=False))

transitions["unc2"] = transitions["unc1"]
transitions["L3'"] = abs(transitions["L3'"])
transitions["L4'"] = abs(transitions["L4'"])
transitions["L3\""] = abs(transitions["L3\""])
transitions["L4\""] = abs(transitions["L4\""])
symmetryToNumbersMap = {
    "A1'":1,
    "A2'":2,
    "E'":3,
    "A1\"":4,
    "A2\"":5,
    "E\"":6  
}
transitions["GammaRot\""] = transitions["GammaRot\""].map(symmetryToNumbersMap)
transitions["GammaVib\""] = transitions["GammaVib\""].map(symmetryToNumbersMap)
transitions["Gamma\""] = transitions["Gamma\""].map(symmetryToNumbersMap)
transitions["Gamma'"] = transitions["Gamma'"].map(symmetryToNumbersMap)
transitions["Tag\""] = transitions["J\""].astype(str) + "-" + transitions["K\""].astype(str) + "-" + transitions["inv\""].astype(str) + "-" +  transitions["GammaVib\""].astype(str) + "-" + transitions["GammaRot\""].astype(str) + "-" + transitions["Gamma\""].astype(str)


statesFileColumns = ["i", "E", "g", "J", "weight", "p", "Gamma", "Nb", "nu1", "nu2", "nu3", "nu4", "L3", "L4", "inv", "J'", "K'", "pRot", "v1", "v2", "v3", "v4", "v5", "v6", "GammaVib", "Calc"]
states = pd.read_csv("../14N-1H3__CoYuTe.states", delim_whitespace=True, names=statesFileColumns)
states = states[states["E"] < 20000]
states = states[states["g"] > 0]
states = states[states["J"] <= 8] 

def findBlockNumber(row, states):
    matchingLowerStates = states[states["J"] == row["J\""]]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["Gamma"] == row["Gamma\""]]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["nu1"] == 0]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["nu3"] == 0]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["nu3"] == 0]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["nu4"] == 0]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["L3"] == 0]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["L4"] == 0]
    # matchingLowerStates = matchingLowerStates[matchingLowerStates["v1"] == 0]
    # matchingLowerStates = matchingLowerStates[matchingLowerStates["v3"] == 0]
    # matchingLowerStates = matchingLowerStates[matchingLowerStates["v3"] == 0]
    # matchingLowerStates = matchingLowerStates[matchingLowerStates["v4"] == 0]
    # matchingLowerStates = matchingLowerStates[matchingLowerStates["v5"] == 0]
    # matchingLowerStates = matchingLowerStates[matchingLowerStates["v6"] == 0]    
    matchingLowerStates = matchingLowerStates[matchingLowerStates["J"] == row["J\""]]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["K'"] == row["K\""]]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["Gamma"] == row["Gamma\""]]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["pRot"] == row["GammaRot\""]]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["GammaVib"] == row["GammaVib\""]]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["inv"] == row["inv\""]]
    # matchingLowerStates["Tag"] = matchingLowerStates["J"].astype(str) + "-" + matchingLowerStates["K'"].astype(str) + "-" + matchingLowerStates["inv"].astype(str) + "-" + matchingLowerStates["GammaVib"].astype(str) + "-" + matchingLowerStates["pRot"].astype(str) + "-" + matchingLowerStates["Gamma"].astype(str)
    # matchingLowerStates = matchingLowerStates[matchingLowerStates["Tag"] == row["Tag\""]]
    matchingLowerState = matchingLowerStates.sort_values(by="E").head(1).squeeze()
    row["E\""] = matchingLowerState["E"]
    row["Nb\""] = matchingLowerState["Nb"]
    row["E'"] = row["E\""] + row["nu"]
    matchingUpperStates = states[states["J"] == row["J'"]]
    matchingUpperStates = matchingUpperStates[matchingUpperStates["K'"] == row["K'"]]
    matchingUpperStates = matchingUpperStates[matchingUpperStates["Gamma"] == row["Gamma'"]]
    # matchingUpperStates = matchingUpperStates[matchingUpperStates["pRot"] == row["GammaRot'"]]
    # matchingUpperStates = matchingUpperStates[matchingUpperStates["GammaVib"] == row["GammaVib'"]]
    matchingUpperStates["Difference"] = abs(row["E'"] - matchingUpperStates["E"])
    matchingUpperStates = matchingUpperStates.sort_values(by="Difference")
    matchingUpperState = matchingUpperStates.head(1).squeeze()
    row["Nb'"] = matchingUpperState["Nb"]
    row["Calc'"] = matchingUpperState["E"]
    row["nu1'"] = matchingUpperState["nu1"]
    row["nu2'"] = matchingUpperState["nu2"]
    row["nu3'"] = matchingUpperState["nu3"]
    row["nu4'"] = matchingUpperState["nu4"]
    row["L3'"] = matchingUpperState["L3"]
    row["L4'"] = matchingUpperState["L4"]
    row["inv'"] = matchingUpperState["inv"]
    return row

transitions = transitions.parallel_apply(lambda x:findBlockNumber(x, states), axis=1, result_type="expand")

symmetryMapping = {
    1: "A1'",
    2: "A2'",
    3: "E'",
    4: "A1\"",
    5: "A2\"",
    6: "E\""
}
transitions["Gamma\""] = transitions["Gamma\""].map(symmetryMapping)
transitions["Gamma'"] = transitions["Gamma'"].map(symmetryMapping)
inversionMapping = {
    0: "s",
    1: "a"
}
transitions["inv'"] = transitions["inv'"].map(inversionMapping)
transitions["inv\""] = transitions["inv\""].map(inversionMapping)
transitionsColumns = ["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "inv'", "Gamma'", "Nb'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "inv\"", "Gamma\"", "Nb\"", "Source"]

transitionsColumnsComparison = ["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "inv'", "GammaRot'", "GammaVib'", "Gamma'", "Nb'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "inv\"", "Gamma\"", "Nb\"", "Source", "E'", "Calc'"]
transitionsWithStateFileComparison = transitions[transitionsColumnsComparison]
transitionsWithStateFileComparison = transitionsWithStateFileComparison.sort_values(by=["J'", "Gamma'", "E'"])
transitions = transitions[transitionsColumns]
transitions = transitions.to_string(index=False, header=False)
marvelFile = "86CoLeMarvelNew.txt"
with open(marvelFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(transitions)
    
transitionsWithStateFileComparison = transitionsWithStateFileComparison.to_string(index=False, header=False)
comparisonFile = "86CoLeAgainstStatesFile.txt"
with open(comparisonFile, "w+") as fileToWriteTo:
    fileToWriteTo.write(transitionsWithStateFileComparison)