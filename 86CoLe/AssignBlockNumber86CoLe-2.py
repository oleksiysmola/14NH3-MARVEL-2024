import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

transitionsColumns = ["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "inv'", "Gamma'", "Nb'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "inv\"", "Gamma\"", "Nb\"", "Source"]


transitions = pd.read_csv("86CoLeMarvel-MatchedToStatesFile.txt", delim_whitespace=True, names=transitionsColumns)
transitionsNew =  pd.read_csv("../18ZoCoOvKy/18ZoCoOvKyMarvel.txt", delim_whitespace=True, names=transitionsColumns)
transitions = transitions[transitions["nu"] > 0]
transitionsNew = transitionsNew[transitionsNew["nu"] > 0]
print(transitions.head(20).to_string(index=False))

# transitions["unc2"] = transitions["unc1"]
# transitions["L3'"] = abs(transitions["L3'"])
# transitions["L4'"] = abs(transitions["L4'"])
# transitions["L3\""] = abs(transitions["L3\""])
# transitions["L4\""] = abs(transitions["L4\""])
symmetryToNumbersMap = {
    "A1'":1,
    "A2'":2,
    "E'":3,
    "A1\"":4,
    "A2\"":5,
    "E\"":6  
}
inversionMapping = {
    "s": 0,
    "a": 1
}

inversionReverseMapping = {
    0: "s",
    1: "a"
}
# transitions["GammaRot\""] = transitions["GammaRot\""].map(symmetryToNumbersMap)
# transitions["GammaVib\""] = transitions["GammaVib\""].map(symmetryToNumbersMap)
# transitions["Gamma\""] = transitions["Gamma\""].map(symmetryToNumbersMap)
# transitions["Gamma'"] = transitions["Gamma'"].map(symmetryToNumbersMap)
# transitions["Tag\""] = transitions["J\""].astype(str) + "-" + transitions["K\""].astype(str) + "-" + transitions["inv\""].astype(str) + "-" +  transitions["GammaVib\""].astype(str) + "-" + transitions["GammaRot\""].astype(str) + "-" + transitions["Gamma\""].astype(str)


statesFileColumns = ["i", "E", "g", "J", "weight", "p", "Gamma", "Nb", "nu1", "nu2", "nu3", "nu4", "L3", "L4", "inv", "J'", "K'", "pRot", "v1", "v2", "v3", "v4", "v5", "v6", "GammaVib", "Calc"]
states = pd.read_csv("../14N-1H3__CoYuTe.states", delim_whitespace=True, names=statesFileColumns)
states = states[states["E"] < 20000]
states = states[states["g"] > 0]
states = states[states["J"] <= 8] 

def findBlockNumber(row, states):
    matchingLowerStates = states[states["J"] == row["J\""]]
    matchingLowerStates = matchingLowerStates[matchingLowerStates["Gamma"] == symmetryToNumbersMap[row["Gamma\""]]]
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
    matchingLowerStates = matchingLowerStates[matchingLowerStates["inv"] == inversionMapping[row["inv\""]]]
    # matchingLowerStates["Tag"] = matchingLowerStates["J"].astype(str) + "-" + matchingLowerStates["K'"].astype(str) + "-" + matchingLowerStates["inv"].astype(str) + "-" + matchingLowerStates["GammaVib"].astype(str) + "-" + matchingLowerStates["pRot"].astype(str) + "-" + matchingLowerStates["Gamma"].astype(str)
    # matchingLowerStates = matchingLowerStates[matchingLowerStates["Tag"] == row["Tag\""]]
    matchingLowerState = matchingLowerStates.sort_values(by="E").head(1).squeeze()
    row["E\""] = matchingLowerState["E"]
    row["Nb\""] = matchingLowerState["Nb"]
    row["E'"] = row["E\""] + row["nu"]
    matchingUpperStates = states[states["J"] == row["J'"]]
    matchingUpperStates = matchingUpperStates[matchingUpperStates["K'"] == row["K'"]]
    matchingUpperStates = matchingUpperStates[matchingUpperStates["Gamma"] == symmetryToNumbersMap[row["Gamma'"]]]
    # matchingUpperStates = matchingUpperStates[matchingUpperStates["pRot"] == row["GammaRot'"]]
    # matchingUpperStates = matchingUpperStates[matchingUpperStates["GammaVib"] == row["GammaVib'"]]
    matchingUpperStates["Difference"] = abs(row["E'"] - matchingUpperStates["E"])
    matchingUpperStates = matchingUpperStates.sort_values(by="Difference")
    matchingUpperState = matchingUpperStates.head(1).squeeze()
    # row["Nb'"] = matchingUpperState["Nb"]
    row["Calc'"] = matchingUpperState["E"]
    if row["Paper"] == "86CoLe":
        row["nu1'"] = matchingUpperState["nu1"]
        row["nu2'"] = matchingUpperState["nu2"]
        row["nu3'"] = matchingUpperState["nu3"]
        row["nu4'"] = matchingUpperState["nu4"]
        row["L3'"] = matchingUpperState["L3"]
        row["L4'"] = matchingUpperState["L4"]
        row["inv'"] = inversionReverseMapping[matchingUpperState["inv"]]
    return row

transitions["Paper"] = "86CoLe"
transitionsNew["Paper"] = "18ZoCoOvKy"
transitions = transitions.parallel_apply(lambda x:findBlockNumber(x, states), axis=1, result_type="expand")
transitionsNew = transitionsNew.parallel_apply(lambda x:findBlockNumber(x, states), axis=1, result_type="expand")
transitions = pd.concat([transitions, transitionsNew])
print(transitions.head(50)["E'"].to_string(index=False))
transitions = transitions.sort_values("E'")
transitions = transitions.groupby(["J'", "Gamma'"])

def obtainBlockNumbers(dataFrame):
    blockCounter = -100
    nu1 = []
    nu2 = []
    nu3 = []
    nu4 = []
    L3  = []
    L4  = []
    inv = []
    matched = []
    newLevels = dataFrame[dataFrame["Paper"] == "18ZoCoOvKy"]
    if dataFrame.iloc[0]["Paper"] == "18ZoCoOvKy":
        blockNumbers = [dataFrame.iloc[0]["Nb'"]]
        nu1 += [dataFrame.iloc[0]["nu1'"]]
        nu2 += [dataFrame.iloc[0]["nu2'"]]    
        nu3 += [dataFrame.iloc[0]["nu3'"]]
        nu4 += [dataFrame.iloc[0]["nu4'"]]
        L3  += [dataFrame.iloc[0]["L3'"]]
        L4  += [dataFrame.iloc[0]["L4'"]]
        inv += [dataFrame.iloc[0]["inv'"]]
        matched += ["18ZoCoOvKy"]
    else:
        newLevels["Diff"] = abs(newLevels["E'"] - dataFrame.iloc[0]["E'"])
        matchingNewLevels = newLevels[newLevels["Diff"] < 0.05]
        if len(matchingNewLevels) >= 1:
            blockNumbers = [matchingNewLevels.iloc[0]["Nb'"]]
            nu1 += [matchingNewLevels.iloc[0]["nu1'"]]
            nu2 += [matchingNewLevels.iloc[0]["nu2'"]]    
            nu3 += [matchingNewLevels.iloc[0]["nu3'"]]
            nu4 += [matchingNewLevels.iloc[0]["nu4'"]]
            L3  += [matchingNewLevels.iloc[0]["L3'"]]
            L4  += [matchingNewLevels.iloc[0]["L4'"]]
            inv += [matchingNewLevels.iloc[0]["inv'"]]
            matched += [True]
        else:
            blockNumbers = [dataFrame.iloc[0]["Nb'"]]
            nu1 += [dataFrame.iloc[0]["nu1'"]]
            nu2 += [dataFrame.iloc[0]["nu2'"]]    
            nu3 += [dataFrame.iloc[0]["nu3'"]]
            nu4 += [dataFrame.iloc[0]["nu4'"]]
            L3  += [dataFrame.iloc[0]["L3'"]]
            L4  += [dataFrame.iloc[0]["L4'"]]
            inv += [dataFrame.iloc[0]["inv'"]]
            matched += [False]
    if len(dataFrame) >= 2:
        for i in range(1, len(dataFrame)):
            if dataFrame.iloc[i]["E'"] - dataFrame.iloc[i - 1]["E'"] > 0.05:
                blockCounter -= 1
            if dataFrame.iloc[i]["Paper"] == "18ZoCoOvKy":
                blockNumbers += [dataFrame.iloc[i]["Nb'"]]
                nu1 += [dataFrame.iloc[i]["nu1'"]]
                nu2 += [dataFrame.iloc[i]["nu2'"]]    
                nu3 += [dataFrame.iloc[i]["nu3'"]]
                nu4 += [dataFrame.iloc[i]["nu4'"]]
                L3  += [dataFrame.iloc[i]["L3'"]]
                L4  += [dataFrame.iloc[i]["L4'"]]
                inv += [dataFrame.iloc[i]["inv'"]]
                matched += ["18ZoCoOvKy"]
            else:
                newLevels["Diff"] = abs(newLevels["E'"] - dataFrame.iloc[i]["E'"])
                matchingNewLevels = newLevels[newLevels["Diff"] < 0.05]
                if len(matchingNewLevels) >= 1:
                    blockNumbers += [matchingNewLevels.iloc[0]["Nb'"]]
                    nu1 += [matchingNewLevels.iloc[0]["nu1'"]]
                    nu2 += [matchingNewLevels.iloc[0]["nu2'"]]    
                    nu3 += [matchingNewLevels.iloc[0]["nu3'"]]
                    nu4 += [matchingNewLevels.iloc[0]["nu4'"]]
                    L3  += [matchingNewLevels.iloc[0]["L3'"]]
                    L4  += [matchingNewLevels.iloc[0]["L4'"]]
                    inv += [matchingNewLevels.iloc[0]["inv'"]]
                    matched += [True]
                else:
                    blockNumbers += [dataFrame.iloc[i]["Nb'"]]
                    nu1 += [dataFrame.iloc[i]["nu1'"]]
                    nu2 += [dataFrame.iloc[i]["nu2'"]]    
                    nu3 += [dataFrame.iloc[i]["nu3'"]]
                    nu4 += [dataFrame.iloc[i]["nu4'"]]
                    L3  += [dataFrame.iloc[i]["L3'"]]
                    L4  += [dataFrame.iloc[i]["L4'"]]
                    inv += [dataFrame.iloc[i]["inv'"]]
                    matched += [False]
    dataFrame["Nb'"] = blockNumbers
    dataFrame["nu1'"] = nu1
    dataFrame["nu2'"] = nu2
    dataFrame["nu3'"] = nu3
    dataFrame["nu4'"] = nu4
    dataFrame["L3'"] = L3
    dataFrame["L4'"] = L4
    dataFrame["inv'"] = inv
    dataFrame["Matched"] = matched
    return dataFrame

transitions = transitions.parallel_apply(lambda x:obtainBlockNumbers(x))
transitionsColumns = ["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "inv'", "Gamma'", "Nb'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "inv\"", "Gamma\"", "Nb\"", "Source", "E'", "Matched"]
transitionsAll = transitions[transitionsColumns].to_string(index=False, header=False)
# Before discarding the 18ZoCoOvKy transitions print all transitions in succession (for debugging)
marvelFile = "86CoLeMarvelWith18ZoCoOvKy.txt"
with open(marvelFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(transitionsAll)
transitions = transitions[transitions["Paper"] == "86CoLe"]
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
# transitions["inv'"] = transitions["inv'"].map(inversionMapping)
# transitions["inv\""] = transitions["inv\""].map(inversionMapping)

# transitionsColumnsComparison = ["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "inv'", "GammaRot'", "GammaVib'", "Gamma'", "Nb'",
#                       "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "inv\"", "Gamma\"", "Nb\"", "Source", "E'", "Calc'"]
# transitionsWithStateFileComparison = transitions[transitionsColumnsComparison]
# transitionsWithStateFileComparison = transitionsWithStateFileComparison.sort_values(by=["J'", "Gamma'", "E'"])
transitions = transitions[transitionsColumns]
transitions = transitions.sort_values(by="E'")
transitions = transitions.to_string(index=False, header=False)
marvelFile = "86CoLe-MARVEL-Old.txt"
with open(marvelFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(transitions)
    
# transitionsWithStateFileComparison = transitionsWithStateFileComparison.to_string(index=False, header=False)
# comparisonFile = "86CoLeAgainstStatesFile.txt"
# with open(comparisonFile, "w+") as fileToWriteTo:
#     fileToWriteTo.write(transitionsWithStateFileComparison)