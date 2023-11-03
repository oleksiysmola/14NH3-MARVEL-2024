import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

marvelColumns = ["nu1", "nu2", "nu3", "nu4", "L3", "L4", "J", "K", "inv", "Gamma", "Nb", "E", "Uncertainty", "Transitions"]

marvelEnergies = pd.read_csv("14NH3-MarvelEnergies-2020.txt", delim_whitespace=True, names=marvelColumns)

def assignMarvelTags(row):
    row["Tag"] = str(row["J"]) + "-" + str(row["Gamma"]) + "-" + str(row["Nb"])
    return row

marvelEnergies = marvelEnergies.parallel_apply(lambda x:assignMarvelTags(x), result_type="expand", axis=1)

print(marvelEnergies.head(20).to_string(index=False))


transitionsColumns = ["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "inv'", "Gamma'", "Nb'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "inv\"", "Gamma\"", "Nb\"", "Source"]

allTransitions = pd.read_csv("../Marvel-14NH3-2020.txt", delim_whitespace=True, names=transitionsColumns)

transitionsFiles = [
    "../21CaCeBeCa/Assigned21CaCeBeCaMarvel.transitions",
    "../21CeCaCo/Assigned21CeCaCoMarvel.transitions",
    "../22CaCeVaCa/AssignedRecommended22CaCeVaCaMarvel.transitions",
    "../22CaCeVaCaa/AssignedRecommended22CaCeVaCaaMarvel.transitions",
    "../22HuSuTo/22HuSuToMarvel.transitions",
    "../23CaCeVo/Assigned23CaCeVoMarvel.transitions"
]

for transitionFile in transitionsFiles:
    transitionsToAdd = pd.read_csv(transitionFile, delim_whitespace=True, names=transitionsColumns)
    allTransitions = pd.concat([allTransitions, transitionsToAdd])

def removeTransitions(row, transitionsToRemove, transitionsToCorrect):
    if row["Source"] in transitionsToRemove:
        row["nu"] = -row["nu"]
    if row["Source"] in transitionsToCorrect.keys():
        row["nu"] = transitionsToCorrect[row["Source"]]
    return row

transitionsToRemove = [
    "22CaCeVaCa.4941",
    "96BrMa.642",
    "21CaCeBeCa.1674",
    "96BrMa.643",
    "22HuSuTo.1233",
    "93LuHeNi.240",
    "93LuHeNi.348",
    "21CeCaCo.204",
    "22HuSuTo.342", ## Transitions marked with a hash
    "22HuSuTo.343", ## appear to have been assigned in a way
    "22HuSuTo.344", ## that breaks selection rules
    "95KlTaBr.417",
    "22HuSuTo.747", ## Two hashes means they've also been checked by eye
    "22HuSuTo.748", ## to see if their lower state energies have been matched
    "22HuSuTo.749", ## to the states file correctly
    "22HuSuTo.750", ##
    "22HuSuTo.359", #
    "22HuSuTo.360", #
    "22HuSuTo.361", #
    "89UrTuRaGu.894",
    "22HuSuTo.623", #
    "22HuSuTo.624", #
    "22HuSuTo.625", #
    "22HuSuTo.626", #
    "14CeHoVeCa.236",
    "22CaCeVaCa.2052",
    "22HuSuTo.764", #
    "22HuSuTo.765", #
    "22HuSuTo.766", #
    "22HuSuTo.767", #
    "23CaCeVo.44",
    "22HuSuTo.45",
    "22HuSuTo.46",
    "22HuSuTo.47"
]

transitionsToCorrect = {
    "14CeHoVeCa.240": 4275.8599
}

allTransitions = allTransitions.parallel_apply(lambda x:removeTransitions(x, transitionsToRemove, transitionsToCorrect), axis=1, result_type="expand")

# Filtering
Jupper = 7
transitions = allTransitions[allTransitions["nu"] > 0]
transitions = transitions[transitions["J'"] == Jupper]
print(transitions.head(20).to_string(index=False))

def assignStateTags(row):
    row["Tag'"] = str(row["J'"]) + "-" + str(row["Gamma'"]) + "-" + str(row["Nb'"])
    row["Tag\""] = str(row["J\""]) + "-" + str(row["Gamma\""]) + "-" + str(row["Nb\""])
    return row

transitions = transitions.parallel_apply(lambda x:assignStateTags(x), result_type="expand", axis=1)
transitions = transitions.sort_values(by=["J'", "Gamma'", "Nb'"])

def computeUpperState(row, marvelEnergies):
    matchingEnergyLevels = marvelEnergies[marvelEnergies["Tag"] == row["Tag\""]]
    if len(matchingEnergyLevels) == 1:
        row["E\""] = matchingEnergyLevels.squeeze()["E"]
        row["E'"] = row["E\""] + row["nu"]
    else:
        row["E\""] = -10000
    return row

transitions = transitions.parallel_apply(lambda x:computeUpperState(x, marvelEnergies), result_type="expand", axis=1)
transitions = transitions[transitions["E\""] >= 0]

transitionsGroupedByUpperState = transitions.groupby(["Tag'"])
def applyCombinationDifferences(transitionsToUpperState, threshold=0.1):
    transitionsToUpperState["Average E'"] = transitionsToUpperState["E'"].mean()
    transitionsToUpperState["Problem"] = abs(transitionsToUpperState["E'"] - transitionsToUpperState["Average E'"]) > threshold
    # If a problematic transition exists we mark all transitions to this upper state as those we wish to return later
    transitionsToUpperState["Return"] = False
    transitionsToUpperState["Return"] = transitionsToUpperState["Problem"].any()
    return transitionsToUpperState

threshold = 0.1
transitions = transitionsGroupedByUpperState.parallel_apply(lambda x:applyCombinationDifferences(x, threshold))
returnedTransitions = transitions[transitions["Return"]]

print("Returned combination differences:")
print(returnedTransitions[["nu", "unc1", "Tag'", "Tag\"", "Source", "Average E'", "E'", "E\"", "Problem"]].to_string(index=False))

transitionsByUpperStateEnergy = transitions.sort_values(by=["E'"])
targetUpperState = 7075.641107
transitionsByUpperStateEnergy = transitionsByUpperStateEnergy[transitionsByUpperStateEnergy["E'"] > targetUpperState - 1]
transitionsByUpperStateEnergy = transitionsByUpperStateEnergy[targetUpperState + 1 > transitionsByUpperStateEnergy["E'"]]
print(f"Returned upper state energies centred on {targetUpperState}: ")
print(transitionsByUpperStateEnergy[["nu", "unc1", "Tag'", "Tag\"", "Source", "Average E'", "E'", "E\"", "Problem"]].to_string(index=False))

# For when matching to states file is needed
readFromStatesFile = False
if readFromStatesFile:
    statesFileColumns = ["i", "E", "g", "J", "weight", "p", "Gamma", "Nb", "n1", "n2", "n3", "n4", "l3", "l4", "inversion", "J'", "K'", "pRot", "v1", "v2", "v3", "v4", "v5", "v6", "GammaVib", "Calc"]
    states = pd.read_csv("../14N-1H3__CoYuTe.states", delim_whitespace=True, names=statesFileColumns)
    states = states[states["E"] < 7000]
    states = states[states["g"] > 0]
    states = states[states["J"] == Jupper]
    states = states[states["E"] > 6500]
    print(states.to_string(index=False))

    statesList = [
        "21CaCeBeCa.1673",
        "21CaCeBeCa.1674"
    ]

    def findMatchingStates(row, states):
        matchingStates = states[states["J"] == row["J'"]]
        matchingStates = matchingStates[matchingStates["Gamma"] == row["Gamma'"]]
        matchingStates = matchingStates[matchingStates["Nb"] == row["Nb'"]]
        row["CoYuTe E'"] = matchingStates.squeeze()["E"]
        return row
    
    transitions = transitions.parallel_apply(lambda x:findMatchingStates(x, states), axis=1, result_type="expand")
    statesFromList = transitions[transitions["Source"].isin(statesList)]
    print("Selected states with CoYuTe upper state energy:")
    print(statesFromList[["nu", "unc1", "Tag'", "Tag\"", "Source", "Average E'", "E'", "CoYuTe E'", "E\"", "Problem"]].to_string(index=False))


allTransitions = allTransitions.sort_values(by=["nu"])
allTransitions = allTransitions.to_string(index=False, header=False)
marvelFile = "../Marvel-14NH3-Main.txt"
with open(marvelFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(allTransitions)