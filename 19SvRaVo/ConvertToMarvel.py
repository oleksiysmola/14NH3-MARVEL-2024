import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

# Code here is a mess but essentially if the given rotational assignment agrees with Hitran
# obtaining the other quantum numbers is simply a matter of matching to the Hitran line already in
# the MARVEL transition set. If there is disagreement or HITRAN didn't assign the rotational quantum numbers
# then we use the transitition frequency to compute the upper state from the ground state and find the 
# matching upper state allowed by symmetry J selection rules.

newTransitions = pd.read_csv("19SvRaVo.txt", delim_whitespace=True)

transitionsColumns = ["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "inv'", "Gamma'", "Nb'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "inv\"", "Gamma\"", "Nb\"", "Source"]

allTransitions = pd.read_csv("../Marvel-14NH3-Main.txt", delim_whitespace=True, names=transitionsColumns)

quantumNumbers = ["nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "inv'", "Gamma'", "Nb'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "inv\"", "Gamma\"", "Nb\""]

def findNewAssignments(row, quantumNumbers, marvelTransitions):
    if row["J\""] == row["HJ\""] and row["K\""] == row["HK\""]:
        marvelTransitions = marvelTransitions[marvelTransitions["J\""] == row["HJ\""]]
        marvelTransitions = marvelTransitions[marvelTransitions["K\""] == row["HK\""]]
        marvelTransitions["Diff"] = abs(row["Hitran"] - marvelTransitions["nu"])
        marvelTransitions = marvelTransitions.sort_values(by="Diff")
        matchingTransition = marvelTransitions.head(1).squeeze()
        for quantumNumber in quantumNumbers:
            try:
                row[quantumNumber] = int(matchingTransition[quantumNumber])
            except:
                row[quantumNumber] = matchingTransition[quantumNumber]
        row["New"] = False
    else:
        row["New"] = True
        row["nu1'"] = -1
        row["nu2'"] = -1
        row["nu3'"] = -1
        row["nu4'"] = -1
        row["L3'"] = -1
        row["L4'"] = -1
        row["J'"] = -1
        row["K'"] = -1
        row["inv'"] = "Na"
        row["Gamma'"] = "Na"
        row["Nb'"] = -1        
        row["nu1\""] = 0
        row["nu2\""] = 0
        row["nu3\""] = 0
        row["nu4\""] = 0
        row["L3\""] = 0
        row["L4\""] = 0
        row["inv\""] = "Na"
        row["Gamma\""] = "Na"
        row["Nb\""] = -1
        # if row["HJ\""] != -1 and row["HK\""] != -1:
        #     marvelTransitions = marvelTransitions[marvelTransitions["J\""] == row["HJ\""]]
        #     marvelTransitions = marvelTransitions[marvelTransitions["K\""] == row["HK\""]]
        #     marvelTransitions["Diff"] = abs(row["Hitran"] - marvelTransitions["nu"])
        #     marvelTransitions = marvelTransitions.sort_values(by="Diff")
        #     matchingTransition = marvelTransitions.head(1).squeeze()
        #     n = 1
        #     for quantumNumber in quantumNumbers:
        #         if n <= 11:
        #             try:
        #                 row[quantumNumber] = int(matchingTransition[quantumNumber])
        #             except:
        #                 row[quantumNumber] = matchingTransition[quantumNumber]
        #         n += 1
    return row

print(newTransitions.to_string(index=False))
newTransitions = newTransitions.parallel_apply(lambda x:findNewAssignments(x, quantumNumbers, allTransitions), result_type="expand", axis=1)


for quantumNumber in quantumNumbers:
    try:
        newTransitions[quantumNumber] = newTransitions[quantumNumber].astype(int)
    except:
        continue
alreadyAssignedLowerStates = newTransitions[newTransitions["Nb\""] != -1]

def matchLowerStates(row, assignedLowerStates):
    if row["Nb\""] == -1:
        matchingAssignedLowerStates = assignedLowerStates[assignedLowerStates["J\""] == row["J\""]]
        matchingAssignedLowerStates = matchingAssignedLowerStates[matchingAssignedLowerStates["K\""] == row["K\""]]
        matchingAssignedLowerState = matchingAssignedLowerStates.head(1).squeeze()
        row["inv\""] = matchingAssignedLowerState["inv\""]
        row["Nb\""] = matchingAssignedLowerState["Nb\""]
        row["Gamma\""] = matchingAssignedLowerState["Gamma\""]
    return row

newTransitions = newTransitions.parallel_apply(lambda x:matchLowerStates(x, alreadyAssignedLowerStates), axis=1, result_type="expand")

newTransitions["Tag\""] = newTransitions["J\""].astype(str) + "-" + newTransitions["K\""].astype(str) + "-" +  newTransitions["Gamma\""].astype(str) + "-" + newTransitions["Nb\""].astype(str)
uniqueLowerStateTags = newTransitions["Tag\""].unique()
print(uniqueLowerStateTags)

marvelColumns = ["nu1", "nu2", "nu3", "nu4", "L3", "L4", "J", "K", "inv", "Gamma", "Nb", "E", "Uncertainty", "Transitions"]

marvelEnergies = pd.read_csv("../CombinationDifferencesTests/14NH3-MarvelEnergies-2020.txt", delim_whitespace=True, names=marvelColumns)

def assignMarvelTags(row):
    row["Tag"] = str(row["J"]) + "-" + str(row["Gamma"]) + "-" + str(row["Nb"])
    return row

marvelEnergies = marvelEnergies.parallel_apply(lambda x:assignMarvelTags(x), result_type="expand", axis=1)

newTransitions = newTransitions.drop("Tag\"", axis=1)
newTransitions["Tag\""] = newTransitions["J\""].astype(str) + "-" +  newTransitions["Gamma\""].astype(str) + "-" + newTransitions["Nb\""].astype(str)

selectionRules = {
    "A1'": "A1\"", # Technically the nuclear spin statistical weights of the A1 states are zero
    "A1\"": "A1'",
    "A2'": "A2\"",
    "A2\"": "A2'",
    "E'": "E\"",
    "E\"": "E'",
}
def findMatchingMarvelUpperState(row, marvelEnergies, selectionRules):
    matchingLowerStateEnergies = marvelEnergies[marvelEnergies["Tag"] == row["Tag\""]]
    if len(matchingLowerStateEnergies) >= 1:
        row["E\""] = matchingLowerStateEnergies.head(1).squeeze()["E"]
        row["E'"] = row["E\""] + row["nu"]
    else:
        row["E\""] = -10000
        row["E'"] = -10000
    if row["Nb'"] == -1 and row["E\""] > 0:
        matchingMarvelEnergies = marvelEnergies[marvelEnergies["Gamma"] == selectionRules[row["Gamma\""]]]
        matchingMarvelEnergies = matchingMarvelEnergies[matchingMarvelEnergies["J"] <= row["J\""] + 1]
        matchingMarvelEnergies = matchingMarvelEnergies[matchingMarvelEnergies["J"] >= row["J\""] - 1]
        matchingMarvelEnergies["Diff"] = abs(matchingMarvelEnergies["E"] - row["E'"])
        matchingMarvelEnergies = matchingMarvelEnergies.sort_values(by="Diff")
        print("Number of matching Marvel energies for upper state: ", len(matchingMarvelEnergies))
        if len(matchingMarvelEnergies) >= 1:
           matchingMarvelEnergy = matchingMarvelEnergies.head(1).squeeze()
           row["nu1'"] = matchingMarvelEnergy["nu1"]
           row["nu2'"] = matchingMarvelEnergy["nu2"]
           row["nu3'"] = matchingMarvelEnergy["nu3"]
           row["nu4'"] = matchingMarvelEnergy["nu4"]
           row["L3'"] = matchingMarvelEnergy["L3"]
           row["L4'"] = matchingMarvelEnergy["L4"]
           row["J'"] = matchingMarvelEnergy["J"]
           row["K'"] = matchingMarvelEnergy["K"]
           row["inv'"] = matchingMarvelEnergy["inv"]
           row["Gamma'"] = matchingMarvelEnergy["Gamma"]
           row["Nb'"] = matchingMarvelEnergy["Nb"]
    return row

newTransitions = newTransitions.parallel_apply(lambda x:findMatchingMarvelUpperState(x, marvelEnergies, selectionRules), axis=1, result_type="expand") 
statesFileColumns = ["i", "E", "g", "J", "weight", "p", "Gamma", "Nb", "nu1", "nu2", "nu3", "nu4", "L3", "L4", "inv", "J'", "K'", "pRot", "v1", "v2", "v3", "v4", "v5", "v6", "GammaVib", "Calc"]
states = pd.read_csv("../14N-1H3__CoYuTe.states", delim_whitespace=True, names=statesFileColumns)
states = states[states["E"] < 8000]
states = states[states["g"] > 0]
states = states[states["J"] <= 4]   
states = states.parallel_apply(lambda x:assignMarvelTags(x), result_type="expand", axis=1)
newTransitions = newTransitions.parallel_apply(lambda x:findMatchingMarvelUpperState(x, states, selectionRules), axis=1, result_type="expand") 
print(newTransitions.to_string(index=False))
# oldAssignments = newTransitions[newTransitions["New"] == False]
# oldAssignments = pd.merge(oldAssignments, newTransitions[quantumNumbers], on=["J\"", "K\""])
sourceList = [f"19SvRaVo.{i + 1}" for i in range(len(newTransitions))]
newTransitions["Source"] = sourceList
newTransitions = newTransitions[transitionsColumns]
for i in range(3,len(transitionsColumns)):
    try:
        newTransitions[transitionsColumns[i]] = newTransitions[transitionsColumns[i]].astype(int)
    except:
        continue

newTransitions = newTransitions.to_string(index=False, header=False)
marvelFile = "19SvRaVoMarvel.txt"
with open(marvelFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(newTransitions)