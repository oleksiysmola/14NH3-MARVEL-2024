import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)


transitions = pd.read_csv("21ZoBeVaCi-MatchedToLevels-2.txt", delim_whitespace=True)
transitions = transitions[transitions["nu"] > 0]

transitions = transitions.dropna()
transitions["unc1"] = 0.04
transitions["unc2"] = 0.04

quantumNumbers = ["J'", "K'", "J\"", "K\"", "v1'", "v2'", "v3'", "v4'", "v5'", "v6'",
                      "v1\"", "v2\"", "v3\"", "v4\"", "v5\"", "v6\"", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'"]

for quantumNumber in quantumNumbers:
    transitions[quantumNumber] = transitions[quantumNumber].astype(int)

print(transitions.head(20).to_string(index=False))
print(len(transitions))

print(transitions.to_string(index=False))
# symmetryToNumbersMap = {
#     "A1'":1,
#     "A2'":2,
#     "E'":3,
#     "A1\"":4,
#     "A2\"":5,
#     "E\"":6  
# }
# transitions["GammaRot\""] = transitions["GammaRot\""].map(symmetryToNumbersMap)
# transitions["GammaVib\""] = transitions["GammaVib\""].map(symmetryToNumbersMap)
# transitions["Gamma\""] = transitions["Gamma\""].map(symmetryToNumbersMap)
# transitions["Gamma'"] = transitions["Gamma'"].map(symmetryToNumbersMap)
# transitions["Tag\""] = transitions["J\""].astype(str) + "-" + transitions["K\""].astype(str) + "-" + transitions["inv\""].astype(str) + "-" +  transitions["GammaVib\""].astype(str) + "-" + transitions["GammaRot\""].astype(str) + "-" + transitions["Gamma\""].astype(str)


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
    matchingLowerStates = states[states["J"] == row["J\""]]
    # matchingLowerStates = matchingLowerStates[matchingLowerStates["Gamma"] == row["Gamma\""]]
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
    # matchingLowerStates = matchingLowerStates[matchingLowerStates["pRot"] == row["GammaRot\""]]
    # matchingLowerStates = matchingLowerStates[matchingLowerStates["GammaVib"] == row["GammaVib\""]]
    inversionMapping = {
        "s": 0,
        "a": 1
    }
    mapfromInversionNumbers = {
        0: "s",
        1: "a"
    }
    # matchingLowerStates = matchingLowerStates[matchingLowerStates["inv"] == inversionMapping[row["inv\""]]]
    # matchingLowerStates["Tag"] = matchingLowerStates["J"].astype(str) + "-" + matchingLowerStates["K'"].astype(str) + "-" + matchingLowerStates["inv"].astype(str) + "-" + matchingLowerStates["GammaVib"].astype(str) + "-" + matchingLowerStates["pRot"].astype(str) + "-" + matchingLowerStates["Gamma"].astype(str)
    # matchingLowerStates = matchingLowerStates[matchingLowerStates["Tag"] == row["Tag\""]]
    matchingLowerStates["Difference"] = abs(matchingLowerStates["Calc"] - row["Calc\""])
    matchingLowerState = matchingLowerStates.sort_values(by="Difference").head(1).squeeze()
    row["E\""] = matchingLowerState["E"]
    row["nu1\""] = matchingLowerState["nu1"]
    row["nu2\""] = matchingLowerState["nu2"]
    row["nu3\""] = matchingLowerState["nu3"]
    row["nu4\""] = matchingLowerState["nu4"]
    row["L3\""] = matchingLowerState["L3"]
    row["L4\""] = matchingLowerState["L4"]
    row["inv\""] = mapfromInversionNumbers[matchingLowerState["inv"]]
    row["Nb\""] = matchingLowerState["Nb"]
    row["Gamma\""] = matchingLowerState["Gamma"]
    row["E'"] = row["E\""] + row["nu"]
    matchingUpperStates = states[states["J"] == row["J'"]]
    symmetryMapping = {
        1: "A1'",
        2: "A2'",
        3: "E'",
        4: "A1\"",
        5: "A2\"",
        6: "E\""
    }
    lowerGamma = symmetryMapping[row["Gamma\""]]
    upperStateGamma = selectionRules[lowerGamma]
    symmetryLabelsToNumbers = {
        "A1'":1,
        "A2'":2,
        "E'":3,
        "A1\"":4,
        "A2\"":5,
        "E\"":6         
    }
    matchingUpperStates = matchingUpperStates[matchingUpperStates["Gamma"] == symmetryLabelsToNumbers[upperStateGamma]]
    matchingUpperStates = matchingUpperStates[matchingUpperStates["K'"] == row["K'"]]
    # matchingUpperStates = matchingUpperStates[matchingUpperStates["Gamma"] == row["Gamma'"]]
    # matchingUpperStates = matchingUpperStates[matchingUpperStates["pRot"] == row["GammaRot'"]]
    # matchingUpperStates = matchingUpperStates[matchingUpperStates["GammaVib"] == row["GammaVib'"]]
    matchingUpperStates["Difference"] = abs(row["Calc'"] - matchingUpperStates["E"])
    matchingUpperStates = matchingUpperStates.sort_values(by="Difference")
    matchingUpperState = matchingUpperStates.head(1).squeeze()
    row["Nb'"] = matchingUpperState["Nb"]
    # row["Calc'"] = matchingUpperState["E"]
    # row["nu1'"] = matchingUpperState["nu1"]
    # row["nu2'"] = matchingUpperState["nu2"]
    # row["nu3'"] = matchingUpperState["nu3"]
    # row["nu4'"] = matchingUpperState["nu4"]
    # row["L3'"] = matchingUpperState["L3"]
    # row["L4'"] = matchingUpperState["L4"]
    # row["inv'"] = matchingUpperState["inv"]
    row["Gamma'"] = matchingUpperState["Gamma"]
    row["Diff"] = matchingUpperState["Difference"]
    row["CoYuTeEnergy"] = matchingUpperState["E"]
    # if row["nu"] >= 15669.8166:
    #     row["ExpRatio"] = row["int"]/0.422
    #     row["CalcRatio"] = row["Int"]/(1.080000e-23)
    # else:
    #     row["ExpRatio"] = row["int"]/0.05900
    #     row["CalcRatio"] = row["Int"]/(8.790000e-25)
    # row["DiffRatio"] = row["ExpRatio"] - row["CalcRatio"]/row["ExpRatio"]
    return row

transitions = transitions.parallel_apply(lambda x:findBlockNumber(x, states, selectionRules), axis=1, result_type="expand")

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
# transitions["inv'"] = transitions["inv'"].map(inversionMapping)
# transitions["inv\""] = transitions["inv\""].map(inversionMapping)
sourceColumn = [f"21ZoBeVaCi.{i+1}" for i in range(len(transitions))] 
transitions["Source"] = sourceColumn
transitionsColumns = ["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "inv'", "Gamma'", "Nb'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "inv\"", "Gamma\"", "Nb\"", "Source"]
transitionsColumnsForComparison = ["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "inv'", "Gamma'", "Nb'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "inv\"", "Gamma\"", "Nb\"", "Source", "Eobs'","Calc'", "CoYuTeEnergy"]
# transitionsColumnsComparison = ["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "inv'", "Gamma'", "Nb'",
#                       "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "inv\"", "Gamma\"", "Nb\"", "Source", "E'", "Calc'", "Diff", "int", "Int", 
#                       "ExpRatio", "CalcRatio", "DiffRatio"]
# transitionsWithStateFileComparison = transitions[transitionsColumnsComparison]
# transitionsWithStateFileComparison = transitionsWithStateFileComparison.sort_values(by=["J'", "Gamma'", "E'"])
# intensityComparison = transitionsWithStateFileComparison[["nu", "int", "Int"]].sort_values(by="int")
# print(intensityComparison[intensityComparison["nu"] > 15669.8166].tail(50).to_string(index=False))
transitionsForComparison = transitions[transitionsColumnsForComparison]
transitions = transitions[transitionsColumns]
transitions = transitions.to_string(index=False, header=False)
marvelFile = "21ZoBeVaCi-MARVEL.txt"
with open(marvelFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(transitions)
transitionsForComparison["Obs-Calc"] = transitionsForComparison["Eobs'"] - transitionsForComparison["Calc'"]
transitionsForComparison["Obs-Calc-CoYuTe"] = transitionsForComparison["Eobs'"] - transitionsForComparison["CoYuTeEnergy"]
transitionsForComparison = transitionsForComparison.to_string(index=False, header=False)
comparisonFile = "21ZoBeVaCiAgainstStatesFile.txt"
with open(comparisonFile, "w+") as fileToWriteTo:
    fileToWriteTo.write(transitionsForComparison)