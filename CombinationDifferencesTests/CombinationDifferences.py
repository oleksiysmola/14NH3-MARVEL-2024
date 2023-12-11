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
    "../23CaCeVo/Assigned23CaCeVoMarvel.transitions",
    "../23YaDeLa/23YaDeLa.txt",
    "../19SvRaVo/19SvRaVoMarvel.txt",
    # "../86CoLe/86CoLeMarvel.txt",
    "../18ZoCoOvKy/18ZoCoOvKyMarvel.txt",
]

for transitionFile in transitionsFiles:
    transitionsToAdd = pd.read_csv(transitionFile, delim_whitespace=True, names=transitionsColumns)
    allTransitions = pd.concat([allTransitions, transitionsToAdd])

def removeTransitions(row, transitionsToRemove, transitionsToCorrect, transitionsToReassign, badLines, uncertaintyScaleFactor=2, repeatTolerance=3, maximumUncertainty=0.1):
    if row["Source"] in transitionsToRemove:
        row["nu"] = -row["nu"]
    if row["Source"] in transitionsToCorrect.keys():
        row["nu"] = transitionsToCorrect[row["Source"]]
    if row["Source"] in transitionsToReassign.keys():
        numberOfQuantumNumbers = int((len(row) - 4)/2)
        reassignment = transitionsToReassign[row["Source"]]
        upperStateReassignment = reassignment[0]
        lowerStateReassignment = reassignment[1]
        columnLabels = row.index.tolist()
        if upperStateReassignment != None:
            newUpperStateLabels = upperStateReassignment.split("-")
            for i in range(3, 3 + numberOfQuantumNumbers):
                row[columnLabels[i]] = newUpperStateLabels[i - 3]
        if lowerStateReassignment != None:
            newLowerStateLabels = lowerStateReassignment.split("-")
            for i in range(3 + numberOfQuantumNumbers, 3 + 2*numberOfQuantumNumbers):
                row[columnLabels[i]] = newLowerStateLabels[i - 3]
    if row["Source"] in badLines["Line"].tolist():
        matchingBadLines = badLines[badLines["Line"] == row["Source"]]
        badLine = matchingBadLines.tail(1).squeeze()
        if len(matchingBadLines) < repeatTolerance:
            row["unc1"] = badLine["Uncertainty'"]
            row["unc2"] = badLine["Uncertainty'"]
        else:
            if badLine["Ratio"] > uncertaintyScaleFactor:
                row["unc1"] = badLine["Uncertainty'"]
                row["unc2"] = badLine["Uncertainty'"]
            else:
                row["unc1"] = uncertaintyScaleFactor*badLine["Uncertainty"]
                row["unc2"] = uncertaintyScaleFactor*badLine["Uncertainty"]
    if row["unc1"] >= maximumUncertainty:
        if row["nu"] > 0:
            row["nu"] = -row["nu"]
    return row

# List of transitions to be invalidated
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
    "22HuSuTo.47",
    "22HuSuTo.379", #
    "22HuSuTo.380", #
    "22HuSuTo.381", #
    "22HuSuTo.892", #
    "22HuSuTo.893", #
    "22HuSuTo.894", #
    "13DoHiYuTe.10681",
    "96BrMa.646",
    "22HuSuTo.521", #
    "22HuSuTo.522", #
    "22HuSuTo.779", #
    "22HuSuTo.780", #
    "22HuSuTo.781", #
    "22HuSuTo.782", #
    "22HuSuTo.57",
    "22HuSuTo.58",
    "22HuSuTo.59",
    "22HuSuTo.795", #
    "22HuSuTo.796", #
    "22CaCeVaCa.1187",
    "22CaCeVaCa.4947",
    "14CeHoVeCa.138",
    "22CaCeVaCa.1182",
    "22CaCeVaCa.1834",
    "22CaCeVaCa.3206",
    "22CaCeVaCa.4946",
    "96BrMa.139",
    "96BrMa.609",
    "14CeHoVeCa.239",
    "22HuSuTo.65",
    "96BrMa.639",
    "96BrMa.640",
    "14CeHoVeCa.195",
    "96BrMa.645",
    # The above transitions were removed in the first instance of combination difference tests with tolerance 0.1 cm-1
    "22HuSuTo.345", #
    "22HuSuTo.347", #
    "22HuSuTo.348", # 
    # A further check on whether symmetry selection rules are respected invalidated the above three lines
    "21CaCeBeCa.961",
    "22HuSuTo.798",
    "23CaCeVo.1010",
    "23CaCeVo.1070",
    "23CaCeVo.1262",
    "23CaCeVo.1383",
    "23CaCeVo.1457",
    "23CaCeVo.1704",
    "23CaCeVo.1011",
    "23CaCeVo.1071",
    "23CaCeVo.1385",
    "23CaCeVo.1459",
    "23CaCeVo.1705",
    "23CaCeVo.3176",
    "14CeHoVeCa.111",
    "21CaCeBeCa.1589",
    "23CaCeVo.2361",
    "23CaCeVo.2516",
    "22CaCeVaCa.270",
    "22CaCeVaCa.1416",
    "22HuSuTo.1034",
    "23CaCeVo.2124",
    "14CeHoVeCa.44",
    "15BaYuTeCl.173",
    "22CaCeVaCa.1058",
    "13DoHiYuTe.880",
    "13DoHiYuTe.2250",
    "16PeYuPi_S3.888",
    "15BaYuTeCl.1249",
    "19SvRaVo.28",
    "19SvRaVo.13",
    "19SvRaVo.31",
    "19SvRaVo.12",
    "19SvRaVo.10",
    "19SvRaVo.39",
    "19SvRaVo.17",
    "19SvRaVo.19",
    "19SvRaVo.30",
    "19SvRaVo.36",
    "19SvRaVo.46",
    "19SvRaVo.33",
    "19SvRaVo.34",
    "19SvRaVo.35",
    "19SvRaVo.24",
    "19SvRaVo.11",
    "18ZoCoOvKy.283",
    # The above set of transitions were invalidated at a CD threshold of 0.05
    "89UrTuRaGu.476",
    "21CeCaCo.189",
    "89UrTuRaGu.564",
    # After the aforementioned validations here we remove the first set of very bad lines highlighted in MARVEL	
]

transitionsToCorrect = {
    "14CeHoVeCa.240": 4275.8599 # For some reason there was a typo copying from 14CeHoVeCa in the MARVEL 2020 paper
}

# Transitions to reassign in format (Source Tag: [New Upper State Tag, New Lower State Tag])
# Reassignments marked with a # are considered potentially dubious
transitionsToReassign = {
    "21CaCeBeCa.480": ["0-6-0-0-0-0-8-4-s-E'-308", None],
    "21CaCeBeCa.1119": ["0-6-0-0-0-0-8-4-s-E'-308", None],
    "22CaCeVaCa.5190": ["0-6-0-0-0-0-8-4-s-E'-308", None],
    "22CaCeVaCaa.2036": ["0-6-0-0-0-0-8-4-s-E'-308", None],
    "22CaCeVaCaa.4322": ["0-6-0-0-0-0-8-4-s-E'-308", None],
    "21CaCeBeCa.479": ["0-6-0-0-0-0-8-4-s-E'-307", None],
    "18ZoCoOvKy.258": ["5-0-1-1-0-0-1-1-a-A2'-1794", None],
    "18ZoCoOvKy.296": ["5-0-1-1-0-0-1-1-a-A2'-1794", None],
    "18ZoCoOvKy.326": ["6-0-0-0-0-0-3-2-a-E\"-8090", None],
    "18ZoCoOvKy.252": ["6-0-0-0-0-0-3-2-a-E\"-8090", None],
    "18ZoCoOvKy.315": ["5-0-1-1-0-0-3-3-a-E'-7950", None],
}

badLines = pd.read_csv("BadLines.txt", delim_whitespace=True)

allTransitions = allTransitions.parallel_apply(lambda x:removeTransitions(x, transitionsToRemove, transitionsToCorrect, transitionsToReassign, badLines), axis=1, result_type="expand")

# Filtering
Jupper = 4
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

# Tolerance for the combination difference test - adjust accordingly
threshold = 0.05 # cm-1
transitions = transitionsGroupedByUpperState.parallel_apply(lambda x:applyCombinationDifferences(x, threshold))
returnedTransitions = transitions[transitions["Return"]]

print("\n Returned combination differences:")
print(returnedTransitions[["nu", "unc1", "Tag'", "Tag\"", "Source", "Average E'", "E'", "E\"", "Problem"]].to_string(index=False))

transitionsByUpperStateEnergy = transitions.sort_values(by=["E'"])
targetUpperState = 7075.641107
transitionsByUpperStateEnergy = transitionsByUpperStateEnergy[transitionsByUpperStateEnergy["E'"] > targetUpperState - 1]
transitionsByUpperStateEnergy = transitionsByUpperStateEnergy[targetUpperState + 1 > transitionsByUpperStateEnergy["E'"]]
print(f"\n Returned upper state energies centred on {targetUpperState}: ")
print(transitionsByUpperStateEnergy[["nu", "unc1", "Tag'", "Tag\"", "Source", "Average E'", "E'", "E\"", "Problem"]].to_string(index=False))

# For checking if transitions obey symmetry selection rules
runSelectionRulesCheck = False
if runSelectionRulesCheck:
    selectionRules = {
        "A1'": "A1\"", # Technically the nuclear spin statistical weights of the A1 states are zero
        "A1\"": "A1'",
        "A2'": "A2\"",
        "A2\"": "A2'",
        "E'": "E\"",
        "E\"": "E'",
    }

    def selectionRulesCheck(row, selectionRules):
        row["SR Broken"] = False 
        if "MAGIC" not in row["Source"]:
            if row["Gamma\""] != selectionRules[row["Gamma'"]]:
                row["SR Broken"] = True
        return row
    
    transitions = transitions.parallel_apply(lambda x:selectionRulesCheck(x, selectionRules), axis=1, result_type="expand")
    transitionsThatBreakSelectionRules = transitions[transitions["SR Broken"]]
    print("\n Printing transitions which violate selection rules: ")
    print(transitionsThatBreakSelectionRules[["nu", "unc1", "Tag'", "Tag\"", "Source", "Average E'", "E'", "E\"", "Problem"]].to_string(index=False))

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