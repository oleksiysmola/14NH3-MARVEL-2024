import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)



transitions = pd.read_csv("UnmatchedTransitions.txt", delim_whitespace=True)
statesFileColumns = ["i", "E", "g", "J", "weight", "p", "Gamma", "Nb", "nu1", "nu2", "nu3", "nu4", "L3", "L4", "inv", "J'", "K'", "pRot", "v1", "v2", "v3", "v4", "v5", "v6", "GammaVib", "Calc"]
states = pd.read_csv("../14N-1H3__CoYuTe.states", delim_whitespace=True, names=statesFileColumns)
states = states[states["E"] < 14000]
states = states[states["g"] > 0]
states = states[states["J"] <= 9] 

selectionRules = {
    "A1'": "A1\"", # Technically the nuclear spin statistical weights of the A1 states are zero
    "A1\"": "A1'",
    "A2'": "A2\"",
    "A2\"": "A2'",
    "E'": "E\"",
    "E\"": "E'",
}

def findBlockNumber(row, states, selectionRules):
    symmetryMapping = {
        1: "A1'",
        2: "A2'",
        3: "E'",
        4: "A1\"",
        5: "A2\"",
        6: "E\""
    }
    symmetryLabelsToNumbers = {
        "A1'":1,
        "A2'":2,
        "E'":3,
        "A1\"":4,
        "A2\"":5,
        "E\"":6         
    }
    matchingLowerStates = states[states["J"] == row["J\""]]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["Gamma"] == symmetryLabelsToNumbers[row["Gamma\""]]]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["nu1"] == 0]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["nu3"] == 0]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["nu3"] == 0]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["nu4"] == 0]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["L3"] == 0]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["L4"] == 0]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["K'"] == row["K\""]]
    print(matchingLowerStates)
    matchingLowerState = matchingLowerStates.head(1).squeeze()
    lowerStateEnergy = matchingLowerState["E"]
    matchingUpperStates = states[states["J"] == row["J'"]]
    matchingUpperStates = matchingUpperStates[matchingUpperStates["Gamma"] == symmetryLabelsToNumbers[row["Gamma'"]]]
    matchingUpperStates = matchingUpperStates[matchingUpperStates["K'"] == row["K'"]]
    # matchingUpperStates = matchingUpperStates[matchingUpperStates["Gamma"] == row["Gamma'"]]
    # matchingUpperStates = matchingUpperStates[matchingUpperStates["pRot"] == row["GammaRot'"]]
    # matchingUpperStates = matchingUpperStates[matchingUpperStates["GammaVib"] == row["GammaVib'"]]
    matchingUpperStates["Difference"] = abs(row["Calc'"] - matchingUpperStates["E"])
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
    # row["Gamma'"] = matchingUpperState["Gamma"]
    row["Diff"] = matchingUpperState["Difference"]
    row["Eobs'"] = lowerStateEnergy + row["nu"]
    return row

transitions = transitions.parallel_apply(lambda x:findBlockNumber(x, states, selectionRules), axis=1, result_type="expand")

print(transitions.to_string(index=False))

# symmetryMapping = {
#     1: "A1'",
#     2: "A2'",
#     3: "E'",
#     4: "A1\"",
#     5: "A2\"",
#     6: "E\""
# }
# transitions["Gamma\""] = transitions["Gamma\""].map(symmetryMapping)
# transitions["Gamma'"] = transitions["Gamma'"].map(symmetryMapping)
# inversionMapping = {
#     0: "s",
#     1: "a"
# }
# # transitions["inv'"] = transitions["inv'"].map(inversionMapping)
# # transitions["inv\""] = transitions["inv\""].map(inversionMapping)
# sourceColumn = [f"18ZoCoOvKy.{i+1}" for i in range(len(transitions))]
# transitions["Source"] = sourceColumn
# transitionsColumns = ["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "inv'", "Gamma'", "Nb'",
#                       "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "inv\"", "Gamma\"", "Nb\"", "Source"]

# transitionsColumnsComparison = ["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "inv'", "Gamma'", "Nb'",
#                       "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "inv\"", "Gamma\"", "Nb\"", "Source", "E'", "Calc'", "Diff", "int", "Int", 
#                       "ExpRatio", "CalcRatio", "DiffRatio"]
# transitionsWithStateFileComparison = transitions[transitionsColumnsComparison]
# transitionsWithStateFileComparison = transitionsWithStateFileComparison.sort_values(by=["J'", "Gamma'", "E'"])
# intensityComparison = transitionsWithStateFileComparison[["nu", "int", "Int"]].sort_values(by="int")
# print(intensityComparison[intensityComparison["nu"] > 15669.8166].tail(50).to_string(index=False))
# transitions = transitions[transitionsColumns]
# transitions = transitions.to_string(index=False, header=False)
# marvelFile = "18ZoCoOvKyMarvel.txt"
# with open(marvelFile, "w+") as FileToWriteTo:
#     FileToWriteTo.write(transitions)
    
# transitionsWithStateFileComparison = transitionsWithStateFileComparison.to_string(index=False, header=False)
# comparisonFile = "18ZoCoOvKyAgainstStatesFile.txt"
# with open(comparisonFile, "w+") as fileToWriteTo:
#     fileToWriteTo.write(transitionsWithStateFileComparison)