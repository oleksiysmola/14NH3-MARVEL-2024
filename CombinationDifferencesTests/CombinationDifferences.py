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

# Filtering
Jupper = 0
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
transitions = transitions[transitions["Return"]]

print(transitions[["nu", "unc1", "Tag'", "Tag\"", "Source", "Average E'", "E'", "E\"", "Problem"]].head(20).to_string(index=False))

allTransitions = allTransitions.sort_values(by=["nu"])
allTransitions = allTransitions.to_string(index=False, header=False)
marvelFile = "../Marvel-14NH3-Main.txt"
with open(marvelFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(allTransitions)