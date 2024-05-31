import pandas as pd
import re
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
    "../24ZhAgSeSh/24ZhAgSeSh.txt",
    # "../86CoLe/86CoLeMarvel.txt",
    # "../18ZoCoOvKy/18ZoCoOvKyMarvel.txt",
    # "../17BaPoYuTe/17BaPoYuTe-MARVEL.txt",
    # "../16BaYuTeBe/16BaYuTeBe-MARVEL.txt",
]

newTransitions = []
for transitionFile in transitionsFiles:
    transitionsToAdd = pd.read_csv(transitionFile, delim_whitespace=True, names=transitionsColumns)
    newTransitions += [transitionsToAdd]
newTransitions = pd.concat(newTransitions)
allTransitions["New"] = False
newTransitions["New"] = True
allTransitions = pd.concat([allTransitions, newTransitions])

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
        # Allow transitions above 10000 cm-1 to have a larger uncertainty
        if row["nu"] > 0 and row["nu"] < 10000:
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
    "16BaYuTeBe.1954",
    "16BaYuTeBe.627",
    "16BaYuTeBe.1785",
    "16BaYuTeBe.98",
    "16BaYuTeBe.81",
    # The above set of transitions were invalidated at a CD threshold of 0.05
    "89UrTuRaGu.476",
    "21CeCaCo.189",
    "89UrTuRaGu.564",
    # After the aforementioned validations here we remove the first set of very bad lines highlighted in MARVEL	
    "18ZoCoOvKy.300",
    "18ZoCoOvKy.108",
    "18ZoCoOvKy.149",
    "18ZoCoOvKy.173",
    "18ZoCoOvKy.174",
    "18ZoCoOvKy.59",
    "18ZoCoOvKy.60",
    "18ZoCoOvKy.101",
    "18ZoCoOvKy.271",
    "18ZoCoOvKy.133",
    "18ZoCoOvKy.288",
    "18ZoCoOvKy.283",
    "18ZoCoOvKy.257",
    "18ZoCoOvKy.120",
    "18ZoCoOvKy.282",
    # "18ZoCoOvKy.45",
    # "18ZoCoOvKy.124",
    "18ZoCoOvKy.54",
    # "18ZoCoOvKy.147",
    # "18ZoCoOvKy.61",
    "18ZoCoOvKy.201",
    "18ZoCoOvKy.131",
    "18ZoCoOvKy.24",
    "18ZoCoOvKy.168",
    "18ZoCoOvKy.234",
    "18ZoCoOvKy.232",
    "18ZoCoOvKy.71",
    "18ZoCoOvKy.23",
    "18ZoCoOvKy.74",
    "18ZoCoOvKy.227",
    # The above are transitions from 18ZoCoOvky which we cannot find a reasonable match for in the states file
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
    # "18ZoCoOvKy.266": ["5-0-1-0-1-0-2-2-s-A2'-2777", None],
    # "18ZoCoOvKy.279": ["5-0-1-0-1-0-2-2-s-A2'-2777", None],
    "18ZoCoOvKy.63":  ["4-0-1-0-1-0-2-1-a-A2'-2888", None],
    "18ZoCoOvKy.78":  ["4-0-1-0-1-0-2-1-a-A2'-2888", None],
    # "18ZoCoOvKy.321": ["5-0-1-0-1-0-3-1-a-A2'-4108", None],
    # "18ZoCoOvKy.250": ["5-0-1-0-1-0-3-1-a-A2'-4108", None],
    "18ZoCoOvKy.140": ["4-0-1-0-1-0-3-0-a-E\"-4042", None],
    # "18ZoCoOvKy.326": ["5-0-1-0-1-0-3-2-a-E\"-8093", None],
    # "18ZoCoOvKy.252": ["5-0-1-0-1-0-3-2-a-E\"-8093", None],
    # "18ZoCoOvKy.305": ["5-0-1-0-1-0-3-3-a-E'-7929", None],
    # "18ZoCoOvKy.263": ["5-0-1-0-1-0-3-3-a-E'-7929", None],
    "18ZoCoOvKy.293": ["5-0-1-0-1-0-4-3-a-E\"-10322", None],
    "18ZoCoOvKy.187": ["4-0-1-0-1-0-4-3-a-E'-5156", None],
    # "18ZoCoOvKy.212": ["4-0-1-0-1-0-4-2-s-E'-5177", None],
    # "18ZoCoOvKy.195": ["4-0-1-0-1-0-4-2-s-E'-5177", None],
    # "18ZoCoOvKy.18":  ["4-0-1-0-1-0-4-2-s-E'-5177", None],
    # "18ZoCoOvKy.117": ["5-0-0-0-0-0-4-2-s-E'-5179", None],
    "18ZoCoOvKy.19":  ["4-0-1-0-1-0-4-2-s-E'-5179", None],
    # "18ZoCoOvKy.198": ["5-0-0-0-0-0-4-2-s-E'-5179", None],
    # "18ZoCoOvKy.275": ["5-0-1-0-1-0-5-4-a-A2\"-6204", None],
    # "18ZoCoOvKy.331": ["5-0-1-0-1-0-5-4-a-A2\"-6204", None],
    # "18ZoCoOvKy.224": ["5-0-0-0-0-0-7-6-a-E'-8434", None], 
    # "18ZoCoOvKy.69":  ["5-0-0-0-0-0-7-6-a-E'-8434", None],
}

badLines = pd.read_csv("BadLines.txt", delim_whitespace=True)

allTransitions = allTransitions.parallel_apply(lambda x:removeTransitions(x, transitionsToRemove, transitionsToCorrect, transitionsToReassign, badLines), axis=1, result_type="expand")

# Filtering
Jupper = 6
transitions = allTransitions[allTransitions["nu"] > 0]
# transitions = transitions[transitions["J'"] == Jupper]
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

def correctAssignments(dataFrame):
    dataFrame = dataFrame.sort_values("E'")
    problem = []
    newAssignments = []
    oldTransitions = dataFrame[dataFrame["New"] == False]
    if len(dataFrame) > 1:
        if dataFrame.iloc[1]["E'"] - dataFrame.iloc[0]["E'"] < 0.05:
            problem += [True]
            if dataFrame.iloc[0]["New"]:
                oldTransitions["Diff"] = abs(oldTransitions["E'"] - dataFrame.iloc[0]["E'"])
                matchingOldTransitions = oldTransitions[oldTransitions["Diff"] < 0.05]
                matchingOldTransitions = matchingOldTransitions.sort_values("Diff")
                if len(matchingOldTransitions) >= 1:
                    matchingTransition = matchingOldTransitions.iloc[0]
                    newAssignment = "\""+str(dataFrame.iloc[0]["Source"])+"\" : [\"" 
                    newAssignment += str(matchingTransition['nu1\''])+"-"+str(matchingTransition['nu2\''])+"-"
                    newAssignment += str(matchingTransition['nu3\''])+"-"+str(matchingTransition['nu4\''])+"-"
                    newAssignment += str(matchingTransition['L3\''])+"-"+str(matchingTransition['L4\''])+"-"
                    newAssignment += str(matchingTransition['J\''])+"-"+str(matchingTransition['K\''])+"-"
                    newAssignment += str(matchingTransition['inv\''])+"-"+str(matchingTransition['Gamma\''])+"-"
                    newAssignment += str(matchingTransition['Nb\''])+"\", None],"
                    currentAssignment = "\""+str(dataFrame.iloc[0]["Source"])+"\" : [\"" 
                    currentAssignment += str(dataFrame.iloc[0]['nu1\''])+"-"+str(dataFrame.iloc[0]['nu2\''])+"-"
                    currentAssignment += str(dataFrame.iloc[0]['nu3\''])+"-"+str(dataFrame.iloc[0]['nu4\''])+"-"
                    currentAssignment += str(dataFrame.iloc[0]['L3\''])+"-"+str(dataFrame.iloc[0]['L4\''])+"-"
                    currentAssignment += str(dataFrame.iloc[0]['J\''])+"-"+str(dataFrame.iloc[0]['K\''])+"-"
                    currentAssignment += str(dataFrame.iloc[0]['inv\''])+"-"+str(dataFrame.iloc[0]['Gamma\''])+"-"
                    currentAssignment += str(dataFrame.iloc[0]['Nb\''])+"\", None],"
                    if newAssignment == currentAssignment:
                        newAssignments += [None]
                    else:
                        newAssignments += [newAssignment]
                else:
                    newAssignments += [None]
            else:
                newAssignments += [None]
        else: 
            problem += [False]
            newAssignments += [None]
        for i in range(1, len(dataFrame) - 1):
            if dataFrame.iloc[i]["E'"] - dataFrame.iloc[i - 1]["E'"] < 0.05:
                problem += [True]
                if dataFrame.iloc[i]["New"]:
                    oldTransitions["Diff"] = abs(oldTransitions["E'"] - dataFrame.iloc[i]["E'"])
                    matchingOldTransitions = oldTransitions[oldTransitions["Diff"] < 0.05]
                    matchingOldTransitions = matchingOldTransitions.sort_values("Diff")
                    if len(matchingOldTransitions) >= 1:
                        matchingTransition = matchingOldTransitions.iloc[0]
                        newAssignment = "\""+str(dataFrame.iloc[i]["Source"])+"\" : [\"" 
                        newAssignment += str(matchingTransition['nu1\''])+"-"+str(matchingTransition['nu2\''])+"-"
                        newAssignment += str(matchingTransition['nu3\''])+"-"+str(matchingTransition['nu4\''])+"-"
                        newAssignment += str(matchingTransition['L3\''])+"-"+str(matchingTransition['L4\''])+"-"
                        newAssignment += str(matchingTransition['J\''])+"-"+str(matchingTransition['K\''])+"-"
                        newAssignment += str(matchingTransition['inv\''])+"-"+str(matchingTransition['Gamma\''])+"-"
                        newAssignment += str(matchingTransition['Nb\''])+"\", None],"
                        currentAssignment = "\""+str(dataFrame.iloc[i]["Source"])+"\" : [\"" 
                        currentAssignment += str(dataFrame.iloc[i]['nu1\''])+"-"+str(dataFrame.iloc[i]['nu2\''])+"-"
                        currentAssignment += str(dataFrame.iloc[i]['nu3\''])+"-"+str(dataFrame.iloc[i]['nu4\''])+"-"
                        currentAssignment += str(dataFrame.iloc[i]['L3\''])+"-"+str(dataFrame.iloc[i]['L4\''])+"-"
                        currentAssignment += str(dataFrame.iloc[i]['J\''])+"-"+str(dataFrame.iloc[i]['K\''])+"-"
                        currentAssignment += str(dataFrame.iloc[i]['inv\''])+"-"+str(dataFrame.iloc[i]['Gamma\''])+"-"
                        currentAssignment += str(dataFrame.iloc[i]['Nb\''])+"\", None],"
                        if newAssignment == currentAssignment:
                            newAssignments += [None]
                        else:
                            newAssignments += [newAssignment]
                    else:
                        newAssignments += [None]
                else:
                    newAssignments += [None]
            elif dataFrame.iloc[i + 1]["E'"] - dataFrame.iloc[i]["E'"] < 0.05:
                problem += [True]
                if dataFrame.iloc[i]["New"]:
                    oldTransitions["Diff"] = abs(oldTransitions["E'"] - dataFrame.iloc[i]["E'"])
                    matchingOldTransitions = oldTransitions[oldTransitions["Diff"] < 0.05]
                    matchingOldTransitions = matchingOldTransitions.sort_values("Diff")
                    if len(matchingOldTransitions) >= 1:
                        matchingTransition = matchingOldTransitions.iloc[0]
                        newAssignment = "\""+str(dataFrame.iloc[i]["Source"])+"\" : [\"" 
                        newAssignment += str(matchingTransition['nu1\''])+"-"+str(matchingTransition['nu2\''])+"-"
                        newAssignment += str(matchingTransition['nu3\''])+"-"+str(matchingTransition['nu4\''])+"-"
                        newAssignment += str(matchingTransition['L3\''])+"-"+str(matchingTransition['L4\''])+"-"
                        newAssignment += str(matchingTransition['J\''])+"-"+str(matchingTransition['K\''])+"-"
                        newAssignment += str(matchingTransition['inv\''])+"-"+str(matchingTransition['Gamma\''])+"-"
                        newAssignment += str(matchingTransition['Nb\''])+"\", None],"
                        currentAssignment = "\""+str(dataFrame.iloc[i]["Source"])+"\" : [\"" 
                        currentAssignment += str(dataFrame.iloc[i]['nu1\''])+"-"+str(dataFrame.iloc[i]['nu2\''])+"-"
                        currentAssignment += str(dataFrame.iloc[i]['nu3\''])+"-"+str(dataFrame.iloc[i]['nu4\''])+"-"
                        currentAssignment += str(dataFrame.iloc[i]['L3\''])+"-"+str(dataFrame.iloc[i]['L4\''])+"-"
                        currentAssignment += str(dataFrame.iloc[i]['J\''])+"-"+str(dataFrame.iloc[i]['K\''])+"-"
                        currentAssignment += str(dataFrame.iloc[i]['inv\''])+"-"+str(dataFrame.iloc[i]['Gamma\''])+"-"
                        currentAssignment += str(dataFrame.iloc[i]['Nb\''])+"\", None],"
                        if newAssignment == currentAssignment:
                            newAssignments += [None]
                        else:
                            newAssignments += [newAssignment]
                    else:
                        newAssignments += [None]
                else:
                    newAssignments += [None]
            else: 
                 problem += [False]
                 newAssignments += [None]
        if dataFrame.iloc[len(dataFrame) - 1]["E'"] - dataFrame.iloc[len(dataFrame) - 2]["E'"] < 0.05:
            problem += [True]
            if dataFrame.iloc[len(dataFrame) - 1]["New"]:
                oldTransitions["Diff"] = abs(oldTransitions["E'"] - dataFrame.iloc[i]["E'"])
                matchingOldTransitions = oldTransitions[oldTransitions["Diff"] < 0.05]
                matchingOldTransitions = matchingOldTransitions.sort_values("Diff")
                if len(matchingOldTransitions) >= 1:
                    matchingTransition = matchingOldTransitions.iloc[0]
                    newAssignment = "\""+str(dataFrame.iloc[len(dataFrame) - 1]["Source"])+"\" : [\"" 
                    newAssignment += str(matchingTransition['nu1\''])+"-"+str(matchingTransition['nu2\''])+"-"
                    newAssignment += str(matchingTransition['nu3\''])+"-"+str(matchingTransition['nu4\''])+"-"
                    newAssignment += str(matchingTransition['L3\''])+"-"+str(matchingTransition['L4\''])+"-"
                    newAssignment += str(matchingTransition['J\''])+"-"+str(matchingTransition['K\''])+"-"
                    newAssignment += str(matchingTransition['inv\''])+"-"+str(matchingTransition['Gamma\''])+"-"
                    newAssignment += str(matchingTransition['Nb\''])+"\", None],"
                    currentAssignment = "\""+str(dataFrame.iloc[len(dataFrame) - 1]["Source"])+"\" : [\"" 
                    currentAssignment += str(dataFrame.iloc[len(dataFrame) - 1]['nu1\''])+"-"+str(dataFrame.iloc[len(dataFrame) - 1]['nu2\''])+"-"
                    currentAssignment += str(dataFrame.iloc[len(dataFrame) - 1]['nu3\''])+"-"+str(dataFrame.iloc[len(dataFrame) - 1]['nu4\''])+"-"
                    currentAssignment += str(dataFrame.iloc[len(dataFrame) - 1]['L3\''])+"-"+str(dataFrame.iloc[len(dataFrame) - 1]['L4\''])+"-"
                    currentAssignment += str(dataFrame.iloc[len(dataFrame) - 1]['J\''])+"-"+str(dataFrame.iloc[len(dataFrame) - 1]['K\''])+"-"
                    currentAssignment += str(dataFrame.iloc[len(dataFrame) - 1]['inv\''])+"-"+str(dataFrame.iloc[len(dataFrame) - 1]['Gamma\''])+"-"
                    currentAssignment += str(dataFrame.iloc[len(dataFrame) - 1]['Nb\''])+"\", None],"
                    if newAssignment == currentAssignment:
                        newAssignments += [None]
                    else:
                        newAssignments += [newAssignment]
                else:
                    newAssignments += [None]
            else:
                newAssignments += [None]
        else: 
            problem += [False]
            newAssignments += [None]
    else:
        problem += [False]
        newAssignments += [None]
    dataFrame["Problem"] = problem
    dataFrame["Reassignment"] = newAssignments
    return dataFrame

transitions = transitions.groupby(["J'", "Gamma'"])
transitions = transitions.parallel_apply(lambda x: correctAssignments(x))
print(transitions[transitions["Problem"]].to_string(index=False))

marvelFile = "MarvelAll-14NH3-SuggestedAssignments.txt"
transitionsAll = transitions.to_string(index=False, header=False)
with open(marvelFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(transitionsAll)
transitions = transitions[transitions["Reassignment"].notna()]
transitions = transitions["Reassignment"].to_list()
marvelFile = "Marvel-14NH3-SuggestedAssignments.txt"
with open(marvelFile, "w+") as FileToWriteTo:
    for i in range(len(transitions)):
        if "A2\"" in transitions[i]:
            FileToWriteTo.write(re.sub("A2\"", 'A2\\"', transitions[i]))
            FileToWriteTo.write("\n")
        elif "E\"" in transitions[i]:
            FileToWriteTo.write(re.sub("E\"", 'E\\"', transitions[i]))
            FileToWriteTo.write("\n")
        else:
            FileToWriteTo.write(transitions[i])
            FileToWriteTo.write("\n")
