import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

empiricalLevels = pd.read_csv("18ZoCoOvKyEnergyLevels.txt", delim_whitespace=True)

transitionsColumnsComparison = ["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "inv'", "Gamma'", "Nb'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "inv\"", "Gamma\"", "Nb\"", "Source", "E'", "Calc'", "Diff"]

transitions = pd.read_csv("18ZoCoOvKyAgainstStatesFile.txt", names=transitionsColumnsComparison, delim_whitespace=True)

def generateTag(row):
    row["Tag"] = str(row["J'"]) + "-" + str(row["K'"]) + "-" + str(row["inv'"]) + "-" + str(row["nu1'"])
    row["Tag"] = str(row["Tag"]) + "-" + str(row["nu2'"]) + "-" + str(row["nu3'"]) + "-" + str(row["nu4'"]) + "-" + str(row["L3'"]) + "-" + str(row["L4'"])
    return row

empiricalLevels = empiricalLevels.parallel_apply(lambda x:generateTag(x), result_type="expand", axis=1)
transitions = transitions.parallel_apply(lambda x:generateTag(x), result_type="expand", axis=1)

def matchTransitionsToEmpiricalLevels(row, empiricalLevels):
    matchingEmpiricalLevel = empiricalLevels[empiricalLevels["Tag"] == row["Tag"]]
    row["Delete"] = True
    if len(matchingEmpiricalLevel) == 1:
        matchingEmpiricalLevel = matchingEmpiricalLevel.head(1).squeeze()
        row["ETable"] = matchingEmpiricalLevel["obs."]
        row["CalcVar"] = matchingEmpiricalLevel["calc.ref"]
        row["Delete"] = False
    return row

transitions = transitions.parallel_apply(lambda x:matchTransitionsToEmpiricalLevels(x, empiricalLevels), result_type="expand", axis=1)
transitionsDeleted = len(transitions[transitions["Delete"]])
print(f"\n {transitionsDeleted} transitions deleted")
transitions = transitions[transitions["Delete"] == False]

statesFileColumns = ["i", "E", "g", "J", "weight", "p", "Gamma", "Nb", "nu1", "nu2", "nu3", "nu4", "L3", "L4", "inv", "J'", "K'", "pRot", "v1", "v2", "v3", "v4", "v5", "v6", "GammaVib", "Calc"]
states = pd.read_csv("../14N-1H3__CoYuTe.states", delim_whitespace=True, names=statesFileColumns)
states = states[states["E"] < 20000]
states = states[states["g"] > 0]
states = states[states["J"] <= 8]


def findMatchInStatesFile(row, states):
    matchingStates = states[states["J"] == row["J'"]]
    matchingStates["Diff"] = abs(row["CalcVar"] - matchingStates["E"])
    matchingState = matchingStates.sort_values(by="Diff").head(1).squeeze()
    row["Nb'"] = matchingState["Nb"]
    row["CoYuTe"] = matchingState["Calc"]
    return row

transitions = transitions.parallel_apply(lambda x:findMatchInStatesFile(x, states), axis=1, result_type="expand")
transitionsColumnsComparison += ["ETable", "CalcVar"]
transitions = transitions[transitionsColumnsComparison]

transitionsMarvel = transitions[["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "inv'", "Gamma'", "Nb'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "inv\"", "Gamma\"", "Nb\"", "Source"]]
transitionsMarvel = transitionsMarvel.to_string(index=False, header=False)
transitions = transitions.to_string(index=False, header=False)
comparisonFile = "18ZoCoOvKyWithEmpiricalLevels.txt"
with open(comparisonFile, "w+") as fileToWriteTo:
    fileToWriteTo.write(transitions)

marvelFile = "18ZoCoOvKyMarvel.txt"
with open(transitionsMarvel, "w+") as fileToWriteTo:
    fileToWriteTo.write(transitionsMarvel)