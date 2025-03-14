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
    "../23YaOlLa/23YaOlLa.txt",
    "../19SvRaVo/19SvRaVoMarvel.txt",
    "../24ZhAgSeSh/24ZhAgSeSh.txt",
    "../86CoLe/86CoLe-MARVEL.txt",
    "../18ZoCoOvKy/18ZoCoOvKyMarvel.txt",
    "../17BaPoYuTe/17BaPoYuTe-MARVEL.txt",
    "../16BaYuTeBe/16BaYuTeBe-MARVEL.txt",
    "../21ZoBeVaCi/21ZoBeVaCi-MARVEL.txt",
    "../18MaMaMaPa/18MaMaMaPa-MARVEL.txt"
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
                row[columnLabels[i]] = newLowerStateLabels[i - (3 + numberOfQuantumNumbers)]
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
    "89UrTuRaGu.820",
    "89UrTuRaGu.872",
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
    "18ZoCoOvKy.181",
    # The above are transitions from 18ZoCoOvky which we cannot find a reasonable match for in the states file
    "18ZoCoOvKy.121",
    "18ZoCoOvKy.33",
    "21ZoBeVaCi.17",
    "21ZoBeVaCi.12",
    "21ZoBeVaCi.252",
    "21ZoBeVaCi.256",
    "21ZoBeVaCi.103",
    "21ZoBeVaCi.233",
    "21ZoBeVaCi.58",
    "21ZoBeVaCi.59",
    "22HuSuTo.679",
    "22CaCeVaCa.6033",
    "22CaCeVaCa.4877",
    "22CaCeVaCa.2228",
    "86CoLe.53",
    "86CoLe.87",
    "86CoLe.214",
]

transitionsToCorrect = {
    "14CeHoVeCa.240": 4275.8599 # For some reason there was a typo copying from 14CeHoVeCa in the MARVEL 2020 paper
}

# Transitions to reassign in format (Source Tag: [New Upper State Tag, New Lower State Tag])
# Reassignments marked with a # are considered potentially dubious
transitionsToReassign = {
    "21CaCeBeCa.480" : ["0-6-0-0-0-0-8-4-s-E'-308", None],
    "21CaCeBeCa.1119": ["0-6-0-0-0-0-8-4-s-E'-308", None],
    "22CaCeVaCa.5190": ["0-6-0-0-0-0-8-4-s-E'-308", None],
    "22CaCeVaCaa.2036": ["0-6-0-0-0-0-8-4-s-E'-308", None],
    "22CaCeVaCaa.4322": ["0-6-0-0-0-0-8-4-s-E'-308", None],
    "21CaCeBeCa.479": ["0-6-0-0-0-0-8-4-s-E'-307", None],
    # "18ZoCoOvKy.266": ["5-0-1-0-1-0-2-2-s-A2'-2777", None],
    # "18ZoCoOvKy.279": ["5-0-1-0-1-0-2-2-s-A2'-2777", None],
    "18ZoCoOvKy.63":  ["4-0-1-0-1-0-2-1-a-E'-2888", None],
    "18ZoCoOvKy.78":  ["4-0-1-0-1-0-2-1-a-E'-2888", None],
    # "18ZoCoOvKy.321": ["5-0-1-0-1-0-3-1-a-A2'-4108", None],
    # "18ZoCoOvKy.250": ["5-0-1-0-1-0-3-1-a-A2'-4108", None],
    "18ZoCoOvKy.140": ["4-0-1-0-1-0-3-0-a-E\"-4042", None],
    # "18ZoCoOvKy.326": ["5-0-1-0-1-0-3-2-a-E\"-8093", None],
    # "18ZoCoOvKy.252": ["5-0-1-0-1-0-3-2-a-E\"-8093", None],
    # "18ZoCoOvKy.305": ["5-0-1-0-1-0-3-3-a-E'-7929", None],
    # "18ZoCoOvKy.263": ["5-0-1-0-1-0-3-3-a-E'-7929", None],
    "18ZoCoOvKy.293": ["5-0-1-0-1-0-4-0-a-E\"-10322", None],
    "18ZoCoOvKy.187": ["4-0-1-0-1-0-4-3-a-E'-5156", None],
    # "18ZoCoOvKy.212": ["4-0-1-0-1-0-4-2-s-E'-5177", None],
    # "18ZoCoOvKy.195": ["4-0-1-0-1-0-4-2-s-E'-5177", None],
    # "18ZoCoOvKy.18":  ["4-0-1-0-1-0-4-2-s-E'-5177", None],
    # "18ZoCoOvKy.117": ["5-0-0-0-0-0-4-2-s-E'-5179", None],
    "18ZoCoOvKy.19" : ["4-0-1-0-1-0-4-2-s-E'-5179", None],
    # "18ZoCoOvKy.198": ["5-0-0-0-0-0-4-2-s-E'-5179", None],
    # "18ZoCoOvKy.275": ["5-0-1-0-1-0-5-4-a-A2\"-6204", None],
    # "18ZoCoOvKy.331": ["5-0-1-0-1-0-5-4-a-A2\"-6204", None],
    # "18ZoCoOvKy.224": ["5-0-0-0-0-0-7-6-a-E'-8434", None], 
    # "18ZoCoOvKy.69":  ["5-0-0-0-0-0-7-6-a-E'-8434", None],
    "21ZoBeVaCi.36": ["2-1-2-0-2-0-3-1-a-A2'-922", None],
    "21ZoBeVaCi.67": ["2-1-2-0-2-0-3-1-a-A2'-922", None],
    "21ZoBeVaCi.190": ["2-1-2-0-2-0-3-1-a-A2'-922", None],
    "21ZoBeVaCi.96": ["0-0-4-0-2-0-4-4-s-A2'-1092", None],
    "21ZoBeVaCi.139": ["0-0-4-0-2-0-4-4-s-A2'-1092", None],
    "21ZoBeVaCi.238": ["0-0-4-0-2-0-4-4-s-A2'-1092", None],
    "86CoLe.56": ["4-0-1-0-1-0-2-1-a-E'-2888", None],
    "86CoLe.79": ["4-0-1-0-1-0-3-0-a-E\"-4042", None],
    "86CoLe.69":  ["4-0-1-0-1-0-4-1-s-A2\"-2637", None],
    "86CoLe.196": ["4-0-1-0-1-0-4-1-s-A2\"-2637", None],
    "86CoLe.68": ["5-0-0-0-0-0-4-0-a-A2\"-2641", None],
    "86CoLe.195": ["5-0-0-0-0-0-4-0-a-A2\"-2641", None],
    "16BaYuTeBe.1462": ["1-2-0-2-0-2-4-2-s-E'-490", None],
    "16BaYuTeBe.1066": ["2-0-0-1-0-1-8-2-s-A2'-413", None],
    # "86CoLe.255": ["5-0-0-0-0-0-5-0-s-A2'-3223", None],
    # "86CoLe.263": ["1-4-1-4-1-3-5-1-a-A2'-3223", None],
    "86CoLe.301": ["0-1-1-7-1-5-6-3-s-E\"-7434", None],
    "86CoLe.132": ["1-4-1-3-1-3-5-1-a-A2'-3223", None],
    # "86CoLe.87": ["4-0-1-0-1-0-2-1-a-E'-2888", None],
    "22HuSuTo.1180" : [None, "0-0-0-0-0-0-4-2-a-E\"-2"], # Lower state appears to have been assigned wrong inversion number
    "22HuSuTo.692"  : [None, "0-0-0-0-0-0-9-5-a-E'-3"], # Lower state appears to have been assigned wrong inversion number
    "22HuSuTo.239"  : ["0-1-1-1-1-1-7-1-s-E\"-272", None],
    "22HuSuTo.238"  : ["0-1-1-1-1-1-7-1-s-E\"-272", None],
    "22HuSuTo.237"  : ["0-1-1-1-1-1-7-1-s-E\"-272", None],
    "22CaCeVaCa.1539" : ["0-1-0-2-0-2-1-0-s-A2'-13", None],     # Assignments made by Cacciani 
    "22CaCeVaCa.1060" : ["0-1-0-2-0-2-1-0-s-A2'-13", None],     # to a certain J-Symmetry block and
    "22CaCeVaCa.1364" : ["0-1-0-2-0-2-1-1-s-E\"-19", None],     # block number but disagree with
    "22CaCeVaCa.1048" : ["0-1-0-2-0-2-1-1-s-E\"-19", None],     # previous assignments
    "22CaCeVaCaa.2829" : ["1-0-0-1-0-1-1-1-s-E\"-37", None],
    "22CaCeVaCaa.2672" : ["1-0-0-1-0-1-1-1-s-E\"-37", None],
    "22CaCeVaCa.1900" : ["0-1-0-2-0-2-1-1-a-E'-20", None],
    "22CaCeVaCa.1523" : ["0-1-0-2-0-2-1-1-a-E'-20", None],
    "22CaCeVaCa.1644" : ["0-1-0-2-0-2-2-0-a-A2\"-20", None],
    "22CaCeVaCa.2369" : ["0-1-0-2-0-2-2-0-a-A2\"-20", None],
    "22CaCeVaCa.1370" : ["0-1-0-2-0-2-2-0-a-A2\"-20", None],
    "22CaCeVaCa.1740" : ["0-1-0-2-0-2-2-1-s-E\"-31", None],
    "22CaCeVaCa.1383" : ["0-1-0-2-0-2-2-1-s-E\"-31", None],
    "22CaCeVaCa.898" : ["0-1-0-2-0-2-2-1-s-E\"-31", None],
    "22CaCeVaCa.1901" : ["0-1-0-2-0-2-2-2-a-E\"-33", None],
    "22CaCeVaCa.1351" : ["0-1-0-2-0-2-2-2-a-E\"-33", None],
    "22CaCeVaCa.1363" : ["0-1-0-2-0-2-2-2-s-E'-32", None],
    "22CaCeVaCa.880" : ["0-1-0-2-0-2-2-2-s-E'-32", None],
    "22CaCeVaCa.2365" : ["0-1-0-2-0-2-2-1-a-E'-35", None],
    "22CaCeVaCa.1923" : ["0-1-0-2-0-2-2-1-a-E'-35", None],
    "22CaCeVaCa.1448" : ["0-1-0-2-0-2-2-1-a-E'-35", None],
    "22CaCeVaCa.1365" : ["0-1-0-2-0-2-2-1-a-E'-35", None],
    "22CaCeVaCa.1356" : ["0-1-0-2-0-2-3-3-s-A2\"-19", None],
    "22CaCeVaCa.660" : ["0-1-0-2-0-2-3-3-s-A2\"-19", None],
    "22CaCeVaCa.1722" : ["0-1-0-2-0-2-3-0-s-A2'-26", None],
    "22CaCeVaCa.1995" : ["0-1-0-2-0-2-3-0-s-A2'-26", None],
    "22CaCeVaCa.1059" : ["0-1-0-2-0-2-3-0-s-A2'-26", None],
    "22CaCeVaCa.745" : ["0-1-0-2-0-2-3-0-s-A2'-26", None],
    "22CaCeVaCa.1908" : ["0-1-0-2-0-2-3-3-a-A2'-28", None],
    "22CaCeVaCa.2204" : ["0-1-0-2-0-2-3-3-a-A2'-28", None],
    "22CaCeVaCa.1200" : ["0-1-0-2-0-2-3-3-a-A2'-28", None],
    "22CaCeVaCa.988" : ["1-0-0-1-0-1-3-1-a-A2'-49", None],
    "22CaCeVaCaa.3545" : ["1-0-0-1-0-1-3-1-a-A2'-49", None],
    "22CaCeVaCaa.3865" : ["1-0-0-1-0-1-3-1-a-A2'-49", None],
    "22CaCeVaCaa.2500" : ["1-0-0-1-0-1-3-1-a-A2'-49", None],
    "22CaCeVaCaa.2071" : ["1-0-0-1-0-1-3-1-a-A2'-49", None],
    "22CaCeVaCaa.2794" : ["0-0-1-2-1-2-3-1-a-A2'-94", None],
    "22CaCeVaCa.1997" : ["0-1-0-2-0-2-3-1-s-E\"-45", None],
    "22CaCeVaCa.1417" : ["0-1-0-2-0-2-3-1-s-E\"-45", None],
    "22CaCeVaCa.865" : ["0-1-0-2-0-2-3-1-s-E\"-45", None],
    "22CaCeVaCa.1235" : ["0-1-0-2-0-2-3-1-s-E\"-45", None],
    "22CaCeVaCa.746" : ["0-1-0-2-0-2-3-1-s-E\"-45", None],
    "22CaCeVaCa.2611" : ["0-1-0-2-0-2-3-2-a-E\"-47", None],
    "22CaCeVaCa.1933" : ["0-1-0-2-0-2-3-2-a-E\"-47", None],
    "22CaCeVaCa.1220" : ["0-1-0-2-0-2-3-2-a-E\"-47", None],
    "22CaCeVaCaa.3607" : ["1-0-0-1-0-1-3-3-s-E\"-79", None],
    "22CaCeVaCaa.2810" : ["1-0-0-1-0-1-3-3-s-E\"-79", None],
    "22CaCeVaCaa.1813" : ["1-0-0-1-0-1-3-3-s-E\"-79", None],
    "22CaCeVaCaa.3975" : ["0-4-0-1-0-1-3-3-a-E\"-85", None],
    "22CaCeVaCaa.3375" : ["0-4-0-1-0-1-3-3-a-E\"-85", None],
    "22CaCeVaCaa.3225" : ["0-4-0-1-0-1-3-3-a-E\"-85", None],
    "22CaCeVaCaa.2345" : ["0-4-0-1-0-1-3-3-a-E\"-85", None],
    "22CaCeVaCaa.2199" : ["0-4-0-1-0-1-3-3-a-E\"-85", None],
    "22CaCeVaCa.1964" : ["0-1-0-2-0-2-3-2-s-E'-42", None],
    "22CaCeVaCa.1389" : ["0-1-0-2-0-2-3-2-s-E'-42", None],
    "22CaCeVaCa.699" : ["0-1-0-2-0-2-3-2-s-E'-42", None],
    "22CaCeVaCa.2615" : ["0-1-0-2-0-2-3-1-a-E'-46", None],
    "22CaCeVaCa.1938" : ["0-1-0-2-0-2-3-1-a-E'-46", None],
    "22CaCeVaCa.1674" : ["0-1-0-2-0-2-3-1-a-E'-46", None],
    "22CaCeVaCa.1296" : ["0-1-0-2-0-2-3-1-a-E'-46", None],
    "22CaCeVaCa.1221" : ["0-1-0-2-0-2-3-1-a-E'-46", None],
    "22CaCeVaCaa.3601" : ["1-0-0-1-0-1-3-3-a-E'-78", None],
    "22CaCeVaCaa.2807" : ["1-0-0-1-0-1-3-3-a-E'-78", None],
    "22CaCeVaCaa.1810" : ["1-0-0-1-0-1-3-3-a-E'-78", None],
    "22CaCeVaCaa.4108" : ["1-0-0-1-0-1-3-1-a-E'-83", None],
    "22CaCeVaCaa.3980" : ["1-0-0-1-0-1-3-1-a-E'-83", None],
    "22CaCeVaCaa.3357" : ["1-0-0-1-0-1-3-1-a-E'-83", None],
    "22CaCeVaCaa.3229" : ["1-0-0-1-0-1-3-1-a-E'-83", None],
    "22CaCeVaCaa.2327" : ["1-0-0-1-0-1-3-1-a-E'-83", None],
    "22CaCeVaCaa.2202" : ["1-0-0-1-0-1-3-1-a-E'-83", None],
    "22CaCeVaCa.2192" : ["0-1-0-2-0-2-4-3-s-A2\"-31", None],
    "22CaCeVaCa.1391" : ["0-1-0-2-0-2-4-3-s-A2\"-31", None],
    "22CaCeVaCa.1816" : ["0-1-0-2-0-2-4-3-s-A2\"-31", None],
    "22CaCeVaCa.525" : ["0-1-0-2-0-2-4-3-s-A2\"-31", None],
    "22CaCeVaCa.285" : ["0-1-0-2-0-2-4-3-s-A2\"-31", None],
    "22CaCeVaCa.3255" : ["0-1-0-2-0-2-4-0-a-A2\"-34", None],
    "22CaCeVaCa.2333" : ["0-1-0-2-0-2-4-0-a-A2\"-34", None],
    "22CaCeVaCa.2871" : ["0-1-0-2-0-2-4-0-a-A2\"-34", None],
    "22CaCeVaCa.1339" : ["0-1-0-2-0-2-4-0-a-A2\"-34", None],
    "22CaCeVaCa.1087" : ["0-1-0-2-0-2-4-0-a-A2\"-34", None],
    "22CaCeVaCa.2868" : ["0-1-0-2-0-2-4-3-a-A2'-27", None],
    "22CaCeVaCa.1965" : ["0-1-0-2-0-2-4-3-a-A2'-27", None],
    "22CaCeVaCa.1083" : ["0-1-0-2-0-2-4-3-a-A2'-27", None],
    "22CaCeVaCa.1606" : ["0-1-0-2-0-2-4-3-a-A2'-27", None],
    "22CaCeVaCaa.2747" : ["1-0-0-1-0-1-4-4-a-A2'-42", None],
    "22CaCeVaCaa.1755" : ["1-0-0-1-0-1-4-4-a-A2'-42", None],
    "22CaCeVaCaa.714" : ["1-0-0-1-0-1-4-4-a-A2'-42", None],
    "22CaCeVaCaa.4547" : ["1-0-0-1-0-1-4-1-a-A2'-50", None],
    "22CaCeVaCaa.3558" : ["1-0-0-1-0-1-4-1-a-A2'-50", None],
    "22CaCeVaCaa.3105" : ["1-0-0-1-0-1-4-1-a-A2'-50", None],
    "22CaCeVaCa.2534" : ["1-0-0-1-0-1-4-1-a-A2'-50", None],
    "22CaCeVaCa.1651" : ["1-0-0-1-0-1-4-1-a-A2'-50", None],
    "22CaCeVaCa.2288" : ["0-1-0-2-0-2-4-1-s-E\"-58", None],
    "22CaCeVaCa.1570" : ["0-1-0-2-0-2-4-1-s-E\"-58", None],
    "22CaCeVaCa.2027" : ["0-1-0-2-0-2-4-1-s-E\"-58", None],
    "22CaCeVaCa.1467" : ["0-1-0-2-0-2-4-1-s-E\"-58", None],
    "22CaCeVaCa.1128" : ["0-1-0-2-0-2-4-1-s-E\"-58", None],
    "22CaCeVaCa.604" : ["0-1-0-2-0-2-4-1-s-E\"-58", None],
    "22CaCeVaCa.2877" : ["0-1-0-2-0-2-4-2-a-E\"-61", None],
    "22CaCeVaCa.1974" : ["0-1-0-2-0-2-4-2-a-E\"-61", None],
    "22CaCeVaCa.1837" : ["0-1-0-2-0-2-4-2-a-E\"-61", None],
    "22CaCeVaCa.1735" : ["0-1-0-2-0-2-4-2-a-E\"-61", None],
    "22CaCeVaCa.1095" : ["0-1-0-2-0-2-4-2-a-E\"-61", None],
    "22CaCeVaCa.989" : ["0-1-0-2-0-2-4-2-a-E\"-61", None],
    "22CaCeVaCaa.4363" : ["0-0-1-1-1-1-4-1-s-E\"-110", None], # Block Numbers manually swapped to correct energy ordering 
    "22CaCeVaCaa.4219" : ["0-0-1-1-1-1-4-1-s-E\"-110", None], # Block Numbers manually swapped to correct energy ordering 
    "22CaCeVaCaa.3366" : ["0-0-1-1-1-1-4-1-s-E\"-110", None], # Block Numbers manually swapped to correct energy ordering 
    "22CaCeVaCaa.3218" : ["0-0-1-1-1-1-4-1-s-E\"-110", None], # Block Numbers manually swapped to correct energy ordering 
    "22CaCeVaCaa.2081" : ["0-0-1-1-1-1-4-1-s-E\"-110", None], # Block Numbers manually swapped to correct energy ordering 
    "22CaCeVaCaa.2649" : ["0-0-1-1-1-1-4-1-s-E\"-110", None], # Block Numbers manually swapped to correct energy ordering 
    "22CaCeVaCaa.1959" : ["0-0-1-1-1-1-4-1-s-E\"-110", None], # Block Numbers manually swapped to correct energy ordering
          "96BrMa.528" : ["0-0-1-1-1-1-4-1-s-E\"-110", None], # Block Numbers manually swapped to correct energy ordering
          "96BrMa.193" : ["0-0-1-1-1-1-4-1-s-E\"-110", None], # Block Numbers manually swapped to correct energy ordering
          "96BrMa.382" : ["0-0-1-1-1-1-4-1-s-E\"-110", None], # Block Numbers manually swapped to correct energy ordering
          "96BrMa.356" : ["0-0-1-1-1-1-4-1-s-E\"-110", None], # Block Numbers manually swapped to correct energy ordering
          "96BrMa.226" : ["0-0-1-1-1-1-4-1-s-E\"-110", None], # Block Numbers manually swapped to correct energy ordering
          "96BrMa.505" : ["0-0-1-1-1-1-4-1-s-E\"-110", None], # Block Numbers manually swapped to correct energy ordering 
    "22CaCeVaCaa.4364" : ["0-0-1-1-1-1-4-0-a-E\"-111", None], # Block Numbers manually swapped to correct energy ordering 
    "22CaCeVaCaa.4220" : ["0-0-1-1-1-1-4-0-a-E\"-111", None], # Block Numbers manually swapped to correct energy ordering 
    "22CaCeVaCaa.3367" : ["0-0-1-1-1-1-4-0-a-E\"-111", None], # Block Numbers manually swapped to correct energy ordering 
    "22CaCeVaCaa.3219" : ["0-0-1-1-1-1-4-0-a-E\"-111", None], # Block Numbers manually swapped to correct energy ordering 
    "22CaCeVaCaa.2083" : ["0-0-1-1-1-1-4-0-a-E\"-111", None], # Block Numbers manually swapped to correct energy ordering 
    "22CaCeVaCaa.2650" : ["0-0-1-1-1-1-4-0-a-E\"-111", None], # Block Numbers manually swapped to correct energy ordering 
    "22CaCeVaCaa.1961" : ["0-0-1-1-1-1-4-0-a-E\"-111", None], # Block Numbers manually swapped to correct energy ordering
          "96BrMa.227" : ["0-0-1-1-1-1-4-0-a-E\"-111", None], # Block Numbers manually swapped to correct energy ordering
          "96BrMa.383" : ["0-0-1-1-1-1-4-0-a-E\"-111", None], # Block Numbers manually swapped to correct energy ordering
          "96BrMa.529" : ["0-0-1-1-1-1-4-0-a-E\"-111", None], # Block Numbers manually swapped to correct energy ordering
    "22CaCeVaCaa.4554" : ["0-0-1-1-1-1-4-1-a-E\"-115", None],
    "22CaCeVaCaa.4397" : ["0-0-1-1-1-1-4-1-a-E\"-115", None],
    "22CaCeVaCaa.3566" : ["0-0-1-1-1-1-4-1-a-E\"-115", None],
    "22CaCeVaCaa.3404" : ["0-0-1-1-1-1-4-1-a-E\"-115", None],
    "22CaCeVaCaa.2272" : ["0-0-1-1-1-1-4-1-a-E\"-115", None],
    "22CaCeVaCaa.2832" : ["0-0-1-1-1-1-4-1-a-E\"-115", None],
    "22CaCeVaCa.1349" : ["0-1-0-2-0-2-4-4-s-E'-55", None],
    "22CaCeVaCa.469" : ["0-1-0-2-0-2-4-4-s-E'-55", None],
    "22CaCeVaCa.2236" : ["0-1-0-2-0-2-4-2-s-E'-57", None],
    "22CaCeVaCa.1430" : ["0-1-0-2-0-2-4-2-s-E'-57", None],
    "22CaCeVaCa.1350" : ["0-1-0-2-0-2-4-2-s-E'-57", None],
    "22CaCeVaCa.1263" : ["0-1-0-2-0-2-4-2-s-E'-57", None],
    "22CaCeVaCa.473" : ["0-1-0-2-0-2-4-2-s-E'-57", None],
    "22CaCeVaCa.560" : ["0-1-0-2-0-2-4-2-s-E'-57", None],
    "22CaCeVaCa.2852" : ["0-1-0-2-0-2-4-1-a-E'-61", None],
    "22CaCeVaCa.2570" : ["0-1-0-2-0-2-4-1-a-E'-61", None],
    "22CaCeVaCa.2061" : ["0-1-0-2-0-2-4-1-a-E'-61", None],
    "22CaCeVaCa.1951" : ["0-1-0-2-0-2-4-1-a-E'-61", None],
    "22CaCeVaCa.1516" : ["0-1-0-2-0-2-4-1-a-E'-61", None],
    "22CaCeVaCa.1073" : ["0-1-0-2-0-2-4-1-a-E'-61", None],
    "22CaCeVaCa.1155" : ["0-1-0-2-0-2-4-1-a-E'-61", None],
    "22CaCeVaCaa.2889" : ["1-0-0-1-0-1-4-2-a-E'-99", None],
    "22CaCeVaCaa.1889" : ["1-0-0-1-0-1-4-2-a-E'-99", None],
    "22CaCeVaCaa.1360" : ["1-0-0-1-0-1-4-2-a-E'-99", None],
    "22CaCeVaCaa.789" : ["1-0-0-1-0-1-4-2-a-E'-99", None],
    "22CaCeVaCa.1044" : ["1-0-0-1-0-1-4-3-a-E'-105", None],
    "22CaCeVaCaa.3845" : ["1-0-0-1-0-1-4-3-a-E'-105", None],
    "22CaCeVaCaa.2812" : ["1-0-0-1-0-1-4-3-a-E'-105", None],
    "22CaCeVaCaa.2533" : ["1-0-0-1-0-1-4-3-a-E'-105", None],
    "22CaCeVaCaa.1586" : ["1-0-0-1-0-1-4-3-a-E'-105", None],
    "22CaCeVaCa.2477" : ["0-1-0-2-0-2-5-3-s-A2\"-31", None],
    "22CaCeVaCa.1440" : ["0-1-0-2-0-2-5-3-s-A2\"-31", None],
    "22CaCeVaCa.1185" : ["0-1-0-2-0-2-5-3-s-A2\"-31", None],
    "22CaCeVaCa.401" : ["0-1-0-2-0-2-5-3-s-A2\"-31", None],
    "22CaCeVaCa.1288" : ["0-1-0-2-0-2-5-3-s-A2\"-31", None],
    "22CaCeVaCaa.3956" : ["1-0-0-1-0-1-5-4-a-A2\"-59", None],
    "22CaCeVaCaa.2671" : ["1-0-0-1-0-1-5-4-a-A2\"-59", None],
    "22CaCeVaCaa.1231" : ["1-0-0-1-0-1-5-4-a-A2\"-59", None],
    "22CaCeVaCaa.2443" : ["1-0-0-1-0-1-5-4-a-A2\"-59", None],
    "22CaCeVaCa.984" : ["1-0-0-1-0-1-5-4-a-A2\"-59", None],
    "22CaCeVaCaa.2674" : ["1-1-0-1-0-1-5-5-s-A2\"-92", None],
    "21CaCeBeCa.669" : ["1-1-0-1-0-1-5-5-s-A2\"-92", None],
    "23CaCeVo.3114" : ["1-1-0-1-0-1-5-5-s-A2\"-92", None],
    "23CaCeVo.2946" : ["1-1-0-1-0-1-5-5-s-A2\"-92", None],
    "23CaCeVo.1028" : ["1-1-0-1-0-1-5-5-s-A2\"-92", None],
    "22HuSuTo.465" : ["1-1-0-1-0-1-5-5-s-A2\"-92", None],
    "22CaCeVaCa.520" : ["0-4-0-1-0-1-5-2-s-A2'-71", None],
    "22CaCeVaCa.1160" : ["0-4-0-1-0-1-5-2-s-A2'-71", None],
    "22CaCeVaCaa.4700" : ["0-4-0-1-0-1-5-2-s-A2'-71", None],
    "22CaCeVaCaa.3451" : ["0-4-0-1-0-1-5-2-s-A2'-71", None],
    "22CaCeVaCaa.4251" : ["0-4-0-1-0-1-5-2-s-A2'-71", None],
    "22CaCeVaCaa.1936" : ["0-4-0-1-0-1-5-2-s-A2'-71", None],
    "22CaCeVaCaa.1524" : ["0-4-0-1-0-1-5-2-s-A2'-71", None],
    "22CaCeVaCa.2684" : ["0-0-1-1-1-1-5-3-a-A2'-72", None],
    "22CaCeVaCa.1178" : ["0-0-1-1-1-1-5-3-a-A2'-72", None],
    "22CaCeVaCaa.4722" : ["0-0-1-1-1-1-5-3-a-A2'-72", None],
    "22CaCeVaCaa.3481" : ["0-0-1-1-1-1-5-3-a-A2'-72", None],
    "22CaCeVaCaa.4283" : ["0-0-1-1-1-1-5-3-a-A2'-72", None],
    "22CaCeVaCaa.1967" : ["0-0-1-1-1-1-5-3-a-A2'-72", None],
    "22CaCeVaCaa.1555" : ["0-0-1-1-1-1-5-3-a-A2'-72", None],
    "22CaCeVaCa.1326" : ["0-1-0-2-0-2-5-5-s-E\"-68", None],
    "22CaCeVaCa.309" : ["0-1-0-2-0-2-5-5-s-E\"-68", None],
    "22CaCeVaCa.3149" : ["0-1-0-2-0-2-5-4-a-E\"-72", None],
    "22CaCeVaCa.2507" : ["0-1-0-2-0-2-5-4-a-E\"-72", None],
    "22CaCeVaCa.2019" : ["0-1-0-2-0-2-5-4-a-E\"-72", None],
    "22CaCeVaCa.1462" : ["0-1-0-2-0-2-5-4-a-E\"-72", None],
    "22CaCeVaCa.535" : ["0-1-0-2-0-2-5-4-a-E\"-72", None],
    "22CaCeVaCa.431" : ["0-1-0-2-0-2-5-4-a-E\"-72", None],
    "22CaCeVaCa.954" : ["0-1-0-2-0-2-5-4-a-E\"-72", None],
    "21CeCaCo.65" : ["0-1-0-2-0-2-5-4-a-E\"-72", None],
    "21CeCaCo.153" : ["0-1-0-2-0-2-5-1-s-E\"-73", None],
    "22CaCeVaCa.3263" : ["0-1-0-2-0-2-5-1-s-E\"-73", None],
    "22CaCeVaCa.2603" : ["0-1-0-2-0-2-5-1-s-E\"-73", None],
    "22CaCeVaCa.1639" : ["0-1-0-2-0-2-5-1-s-E\"-73", None],
    "22CaCeVaCa.2115" : ["0-1-0-2-0-2-5-1-s-E\"-73", None],
    "22CaCeVaCa.1536" : ["0-1-0-2-0-2-5-1-s-E\"-73", None],
    "22CaCeVaCa.601" : ["0-1-0-2-0-2-5-1-s-E\"-73", None],
    "22CaCeVaCa.501" : ["0-1-0-2-0-2-5-1-s-E\"-73", None],
    "22CaCeVaCa.1026" : ["0-1-0-2-0-2-5-1-s-E\"-73", None],
    "22CaCeVaCa.3152" : ["0-1-0-2-0-2-5-2-a-E\"-76", None],
    "22CaCeVaCa.3018" : ["0-1-0-2-0-2-5-2-a-E\"-76", None],
    "22CaCeVaCa.2905" : ["0-1-0-2-0-2-5-2-a-E\"-76", None],
    "22CaCeVaCa.2021" : ["0-1-0-2-0-2-5-2-a-E\"-76", None],
    "22CaCeVaCa.1889" : ["0-1-0-2-0-2-5-2-a-E\"-76", None],
    "22CaCeVaCa.958" : ["0-1-0-2-0-2-5-2-a-E\"-76", None],
    "22CaCeVaCa.858" : ["0-1-0-2-0-2-5-2-a-E\"-76", None],
    "22CaCeVaCa.1586" : ["0-1-0-2-0-2-5-2-a-E\"-76", None],
    "21CeCaCo.68" : ["0-1-0-2-0-2-5-2-a-E\"-76", None],
    "22CaCeVaCaa.4945" : ["1-0-0-1-0-1-5-2-a-E\"-135", None],
    "22CaCeVaCaa.4231" : ["1-0-0-1-0-1-5-2-a-E\"-135", None],
    "22CaCeVaCaa.3705" : ["1-0-0-1-0-1-5-2-a-E\"-135", None],
    "22CaCeVaCaa.2960" : ["1-0-0-1-0-1-5-2-a-E\"-135", None],
    "22CaCeVaCaa.1494" : ["1-0-0-1-0-1-5-2-a-E\"-135", None],
    "22CaCeVaCaa.2165" : ["1-0-0-1-0-1-5-2-a-E\"-135", None],
    "22CaCeVaCa.719" : ["1-0-0-1-0-1-5-2-a-E\"-135", None],
    "22CaCeVaCaa.4466" : ["1-0-0-1-0-1-5-0-a-E\"-141", None],
    "22CaCeVaCaa.3213" : ["1-0-0-1-0-1-5-0-a-E\"-141", None],
    "22CaCeVaCaa.1838" : ["1-0-0-1-0-1-5-0-a-E\"-141", None],
    "22CaCeVaCaa.1703" : ["1-0-0-1-0-1-5-0-a-E\"-141", None],
    "22CaCeVaCa.2396" : ["0-1-0-2-0-2-5-4-s-E'-67", None],
    "22CaCeVaCa.1384" : ["0-1-0-2-0-2-5-4-s-E'-67", None],
    "22CaCeVaCa.357" : ["0-1-0-2-0-2-5-4-s-E'-67", None],
    "22CaCeVaCaa.4002" : ["1-0-0-1-0-1-5-4-s-E'-130", None],
    "22CaCeVaCaa.3749" : ["1-0-0-1-0-1-5-4-s-E'-130", None],
    "22CaCeVaCaa.2710" : ["1-0-0-1-0-1-5-4-s-E'-130", None],
    "22CaCeVaCaa.1274" : ["1-0-0-1-0-1-5-4-s-E'-130", None],
    "22CaCeVaCaa.2211" : ["1-0-0-1-0-1-5-4-s-E'-130", None],
    "22CaCeVaCaa.5117" : ["0-0-1-1-1-1-5-1-s-E'-137", None],
    "22CaCeVaCaa.4564" : ["0-0-1-1-1-1-5-1-s-E'-137", None],
    "22CaCeVaCaa.4424" : ["0-0-1-1-1-1-5-1-s-E'-137", None],
    "22CaCeVaCaa.3879" : ["0-0-1-1-1-1-5-1-s-E'-137", None],
    "22CaCeVaCaa.3175" : ["0-0-1-1-1-1-5-1-s-E'-137", None],
    "22CaCeVaCaa.3316" : ["0-0-1-1-1-1-5-1-s-E'-137", None],
    "22CaCeVaCaa.1797" : ["0-0-1-1-1-1-5-1-s-E'-137", None],
    "22CaCeVaCaa.1677" : ["0-0-1-1-1-1-5-1-s-E'-137", None],
    "22CaCeVaCaa.2339" : ["0-0-1-1-1-1-5-1-s-E'-137", None],
    "22CaCeVaCa.1410" : ["0-0-1-1-1-1-5-1-s-E'-139", None],
    "22CaCeVaCaa.5163" : ["0-0-1-1-1-1-5-1-s-E'-139", None],
    "22CaCeVaCaa.4622" : ["0-0-1-1-1-1-5-1-s-E'-139", None],
    "22CaCeVaCaa.3930" : ["0-0-1-1-1-1-5-1-s-E'-139", None],
    "22CaCeVaCaa.3238" : ["0-0-1-1-1-1-5-1-s-E'-139", None],
    "22CaCeVaCaa.3365" : ["0-0-1-1-1-1-5-1-s-E'-139", None],
    "22CaCeVaCaa.1845" : ["0-0-1-1-1-1-5-1-s-E'-139", None],
    "22CaCeVaCaa.1725" : ["0-0-1-1-1-1-5-1-s-E'-139", None],
    "22CaCeVaCaa.2388" : ["0-0-1-1-1-1-5-1-s-E'-139", None],
    "22CaCeVaCa.2759" : ["0-1-0-2-0-2-6-3-s-A2\"-46", None],
    "22CaCeVaCa.313" : ["0-1-0-2-0-2-6-3-s-A2\"-46", None],
    "22CaCeVaCa.2405" : ["0-1-0-2-0-2-6-3-s-A2\"-46", None],
    "22CaCeVaCa.1501" : ["0-1-0-2-0-2-6-3-s-A2\"-46", None],
    "22CaCeVaCa.174" : ["0-1-0-2-0-2-6-3-s-A2\"-46", None],
    "22CaCeVaCa.1190" : ["0-1-0-2-0-2-6-3-s-A2\"-46", None],
    "22CaCeVaCa.786" : ["1-0-0-1-0-1-6-4-a-A2\"-83", None],
    "22CaCeVaCaa.4211" : ["1-0-0-1-0-1-6-4-a-A2\"-83", None],
    "22CaCeVaCaa.1017" : ["1-0-0-1-0-1-6-4-a-A2\"-83", None],
    "22CaCeVaCaa.2678" : ["1-0-0-1-0-1-6-4-a-A2\"-83", None],
    "22CaCeVaCaa.3982" : ["1-0-0-1-0-1-6-4-a-A2\"-83", None],
    "22CaCeVaCaa.2204" : ["1-0-0-1-0-1-6-4-a-A2\"-83", None],
    "22CaCeVaCa.1304" : ["0-1-0-2-0-2-6-6-s-A2'-36", None],
    "22CaCeVaCa.206" : ["0-1-0-2-0-2-6-6-s-A2'-36", None],
    "21CeCaCo.311" : ["0-1-0-2-0-2-6-1-s-E\"-84", None],
    "22CaCeVaCa.3486" : ["0-1-0-2-0-2-6-1-s-E\"-84", None],
    "22CaCeVaCa.2784" : ["0-1-0-2-0-2-6-1-s-E\"-84", None],
    "22CaCeVaCa.1609" : ["0-1-0-2-0-2-6-1-s-E\"-84", None],
    "22CaCeVaCa.405" : ["0-1-0-2-0-2-6-1-s-E\"-84", None],
    "22CaCeVaCa.1517" : ["0-1-0-2-0-2-6-1-s-E\"-84", None],
    "22CaCeVaCa.327" : ["0-1-0-2-0-2-6-1-s-E\"-84", None],
    "22CaCeVaCa.2069" : ["0-1-0-2-0-2-6-1-s-E\"-84", None],
    "22CaCeVaCa.814" : ["0-1-0-2-0-2-6-1-s-E\"-84", None],
    "22CaCeVaCa.5278" : ["0-1-1-0-1-0-6-6-s-E\"-94", None],
    "22CaCeVaCa.4287" : ["0-1-1-0-1-0-6-6-s-E\"-94", None],
    "22CaCeVaCa.2744" : ["0-1-1-0-1-0-6-6-s-E\"-94", None],
    "22CaCeVaCaa.4649" : ["1-0-0-1-0-1-6-2-a-E\"-160", None],
    "22CaCeVaCaa.5187" : ["1-0-0-1-0-1-6-2-a-E\"-160", None],
    "22CaCeVaCaa.4493" : ["1-0-0-1-0-1-6-2-a-E\"-160", None],
    "22CaCeVaCaa.3126" : ["1-0-0-1-0-1-6-2-a-E\"-160", None],
    "22CaCeVaCaa.1401" : ["1-0-0-1-0-1-6-2-a-E\"-160", None],
    "22CaCeVaCaa.2973" : ["1-0-0-1-0-1-6-2-a-E\"-160", None],
    "22CaCeVaCaa.1277" : ["1-0-0-1-0-1-6-2-a-E\"-160", None],
    "22CaCeVaCaa.3718" : ["1-0-0-1-0-1-6-2-a-E\"-160", None],
    "22CaCeVaCaa.1946" : ["1-0-0-1-0-1-6-2-a-E\"-160", None],
    "22CaCeVaCa.533" : ["1-0-0-1-0-1-6-2-a-E\"-160", None],
    "22CaCeVaCa.2678" : ["0-1-0-2-0-2-6-4-s-E'-81", None],
    "22CaCeVaCa.1428" : ["0-1-0-2-0-2-6-4-s-E'-81", None],
    "22CaCeVaCa.275" : ["0-1-0-2-0-2-6-4-s-E'-81", None],
    "22CaCeVaCa.3368" : ["0-1-0-2-0-2-6-1-a-E'-89", None],
    "22CaCeVaCa.2100" : ["0-1-0-2-0-2-6-1-a-E'-89", None],
    "22CaCeVaCa.838" : ["0-1-0-2-0-2-6-1-a-E'-89", None],
    "22CaCeVaCa.1984" : ["0-1-0-2-0-2-6-1-a-E'-89", None],
    "22CaCeVaCa.739" : ["0-1-0-2-0-2-6-1-a-E'-89", None],
    "22CaCeVaCa.2597" : ["0-1-0-2-0-2-6-1-a-E'-89", None],
    "22CaCeVaCa.1218" : ["0-1-0-2-0-2-6-1-a-E'-89", None],
    "21CeCaCo.228" : ["0-1-0-2-0-2-6-1-a-E'-89", None],
    "21CeCaCo.681" : ["0-1-0-2-0-2-6-1-a-E'-89", None],
    "22CaCeVaCa.4903" : ["0-1-1-0-1-0-6-6-a-E'-96", None],
    "22CaCeVaCa.3829" : ["0-1-1-0-1-0-6-6-a-E'-96", None],
    "21CeCaCo.448" : ["0-1-1-0-1-0-6-6-a-E'-96", None],
    "22CaCeVaCa.3669" : ["0-1-1-0-1-0-6-6-a-E'-96", None],
    "21CeCaCo.575" : ["0-1-1-0-1-0-6-6-a-E'-96", None],
    "22CaCeVaCaa.3941" : ["1-0-0-1-0-1-6-6-s-E'-139", None],
    "22CaCeVaCaa.2399" : ["1-0-0-1-0-1-6-6-s-E'-139", None],
    "22CaCeVaCaa.805" : ["1-0-0-1-0-1-6-6-s-E'-139", None],
    "22CaCeVaCa.702" : ["0-0-0-3-0-1-6-5-a-E'-146", None],
    "22CaCeVaCa.929" : ["0-0-0-3-0-1-6-5-a-E'-146", None],
    "22CaCeVaCaa.4072" : ["0-0-0-3-0-1-6-5-a-E'-146", None],
    "22CaCeVaCaa.2530" : ["0-0-0-3-0-1-6-5-a-E'-146", None],
    "22CaCeVaCaa.904" : ["0-0-0-3-0-1-6-5-a-E'-146", None],
    "22CaCeVaCaa.2344" : ["0-0-0-3-0-1-6-5-a-E'-146", None],
    "22CaCeVaCaa.5182" : ["1-0-0-1-0-1-6-2-s-E'-163", None],
    "22CaCeVaCaa.4498" : ["1-0-0-1-0-1-6-2-s-E'-163", None],
    "22CaCeVaCaa.1396" : ["1-0-0-1-0-1-6-2-s-E'-163", None],
    "22CaCeVaCaa.2981" : ["1-0-0-1-0-1-6-2-s-E'-163", None],
    "22CaCeVaCaa.1283" : ["1-0-0-1-0-1-6-2-s-E'-163", None],
    "22CaCeVaCaa.3712" : ["1-0-0-1-0-1-6-2-s-E'-163", None],
    "22CaCeVaCaa.5394" : ["1-0-0-1-0-1-6-1-a-E'-169", None],
    "22CaCeVaCaa.4870" : ["1-0-0-1-0-1-6-1-a-E'-169", None],
    "22CaCeVaCaa.3380" : ["1-0-0-1-0-1-6-1-a-E'-169", None],
    "22CaCeVaCaa.1630" : ["1-0-0-1-0-1-6-1-a-E'-169", None],
    "22CaCeVaCaa.3247" : ["1-0-0-1-0-1-6-1-a-E'-169", None],
    "22CaCeVaCaa.1510" : ["1-0-0-1-0-1-6-1-a-E'-169", None],
    "22CaCeVaCaa.3938" : ["1-0-0-1-0-1-6-1-a-E'-169", None],
    "22CaCeVaCaa.2163" : ["1-0-0-1-0-1-6-1-a-E'-169", None],
    "22CaCeVaCaa.2687" : ["1-0-0-1-0-1-7-4-a-A2\"-88", None],
    "22CaCeVaCaa.4477" : ["1-0-0-1-0-1-7-4-a-A2\"-88", None],
    "22CaCeVaCaa.846" : ["1-0-0-1-0-1-7-4-a-A2\"-88", None],
    "22CaCeVaCaa.5620" : ["1-0-0-1-0-1-7-4-a-A2\"-88", None],
    "22CaCeVaCaa.1977" : ["1-0-0-1-0-1-7-4-a-A2\"-88", None],
    "22CaCeVaCaa.3988" : ["1-0-0-1-0-1-7-4-a-A2\"-88", None],
    "22CaCeVaCa.2043" : ["1-0-0-1-0-1-7-4-a-A2\"-88", None],
    "22CaCeVaCa.573" : ["1-0-0-1-0-1-7-4-a-A2\"-88", None],
    "22CaCeVaCaa.3469" : ["1-0-0-1-0-1-7-2-s-A2\"-93", None],
    "22CaCeVaCaa.5190" : ["1-0-0-1-0-1-7-2-s-A2\"-93", None],
    "22CaCeVaCaa.1478" : ["1-0-0-1-0-1-7-2-s-A2\"-93", None],
    "22CaCeVaCaa.2696" : ["1-0-0-1-0-1-7-4-s-A2'-95", None],
    "22CaCeVaCaa.4484" : ["1-0-0-1-0-1-7-4-s-A2'-95", None],
    "22CaCeVaCaa.855" : ["1-0-0-1-0-1-7-4-s-A2'-95", None],
    "22CaCeVaCaa.5615" : ["1-0-0-1-0-1-7-4-s-A2'-95", None],
    "22CaCeVaCaa.1969" : ["1-0-0-1-0-1-7-4-s-A2'-95", None],
    "22CaCeVaCa.1835" : ["1-0-0-1-0-1-7-6-a-E\"-166", None],
    "22CaCeVaCa.882" : ["1-0-0-1-0-1-7-6-a-E\"-166", None],
    "22CaCeVaCaa.666" : ["1-0-0-1-0-1-7-6-a-E\"-166", None],
    "22CaCeVaCaa.4191" : ["1-0-0-1-0-1-7-6-a-E\"-166", None],
    "22CaCeVaCaa.2405" : ["1-0-0-1-0-1-7-6-a-E\"-166", None],
    "22CaCeVaCaa.2236" : ["1-0-0-1-0-1-7-6-a-E\"-166", None],
    "22CaCeVaCaa.4655" : ["1-0-0-1-0-1-7-3-s-E\"-189", None],
    "22CaCeVaCaa.974" : ["1-0-0-1-0-1-7-3-s-E\"-189", None],
    "22CaCeVaCaa.2879" : ["1-0-0-1-0-1-7-3-s-E\"-189", None],
    "22CaCeVaCaa.5325" : ["1-0-0-1-0-1-7-3-s-E\"-189", None],
    "22CaCeVaCaa.3617" : ["1-0-0-1-0-1-7-3-s-E\"-189", None],
    "22CaCeVaCaa.1606" : ["1-0-0-1-0-1-7-3-s-E\"-189", None],
    "22CaCeVaCaa.5081" : ["0-2-1-0-1-0-7-5-a-E\"-195", None],
    "22CaCeVaCaa.1362" : ["0-2-1-0-1-0-7-5-a-E\"-195", None],
    "22CaCeVaCaa.3345" : ["0-2-1-0-1-0-7-5-a-E\"-195", None],
    "22CaCeVaCaa.4937" : ["0-2-1-0-1-0-7-5-a-E\"-195", None],
    "22CaCeVaCaa.1232" : ["0-2-1-0-1-0-7-5-a-E\"-195", None],
    "22CaCeVaCaa.3188" : ["0-2-1-0-1-0-7-5-a-E\"-195", None],
    "22CaCeVaCaa.5808" : ["0-2-1-0-1-0-7-5-a-E\"-195", None],
    "22CaCeVaCaa.3899" : ["0-2-1-0-1-0-7-5-a-E\"-195", None],
    "22CaCeVaCaa.1875" : ["0-2-1-0-1-0-7-5-a-E\"-195", None],
    "22CaCeVaCaa.268" : ["0-0-1-1-1-1-7-6-s-E'-151", None],
    "22CaCeVaCaa.3411" : ["0-0-1-1-1-1-7-6-s-E'-151", None],
    "22CaCeVaCaa.1658" : ["0-0-1-1-1-1-7-6-s-E'-151", None],
    "22CaCeVaCaa.1473" : ["0-0-1-1-1-1-7-6-s-E'-151", None],
    "22CaCeVaCaa.4032" : ["1-0-0-1-0-1-7-4-a-E'-159", None],
    "22CaCeVaCaa.2253" : ["1-0-0-1-0-1-7-4-a-E'-159", None],
    "22CaCeVaCaa.2062" : ["1-0-0-1-0-1-7-4-a-E'-159", None],
    "22CaCeVaCaa.4188" : ["1-0-0-1-0-1-7-6-s-E'-167", None],
    "22CaCeVaCaa.669" : ["1-0-0-1-0-1-7-6-s-E'-167", None],
    "22CaCeVaCaa.3986" : ["1-0-0-1-0-1-7-6-s-E'-167", None],
    "22CaCeVaCaa.1970" : ["1-0-0-1-0-1-7-6-s-E'-167", None],
    "22CaCeVaCaa.942" : ["1-0-0-1-0-1-7-6-s-E'-167", None],
    "22CaCeVaCaa.3644" : ["1-0-0-1-0-1-7-6-s-E'-167", None],
    "22CaCeVaCaa.1861" : ["1-0-0-1-0-1-7-6-s-E'-167", None],
    "22CaCeVaCaa.376" : ["1-0-0-1-0-1-7-6-s-E'-167", None],
    "22CaCeVaCaa.2835" : ["1-0-0-1-0-1-7-6-s-E'-167", None],
    "22CaCeVaCaa.5130" : ["1-0-0-1-0-1-7-1-s-E'-196", None],
    "22CaCeVaCaa.3401" : ["1-0-0-1-0-1-7-1-s-E'-196", None],
    "22CaCeVaCaa.1409" : ["1-0-0-1-0-1-7-1-s-E'-196", None],
    "22CaCeVaCaa.1297" : ["1-0-0-1-0-1-7-1-s-E'-196", None],
    "22CaCeVaCaa.3260" : ["1-0-0-1-0-1-7-1-s-E'-196", None],
    "22CaCeVaCaa.5592" : ["1-0-0-1-0-1-7-1-s-E'-196", None],
    "22CaCeVaCaa.3952" : ["1-0-0-1-0-1-7-1-s-E'-196", None],
    "22CaCeVaCaa.1948" : ["1-0-0-1-0-1-7-1-s-E'-196", None],
    "22CaCeVaCaa.455" : ["1-0-0-1-0-1-8-7-s-A2\"-93", None],
    "22CaCeVaCaa.2269" : ["1-0-0-1-0-1-8-7-s-A2\"-93", None],
    "22CaCeVaCaa.4280" : ["1-0-0-1-0-1-8-7-s-A2\"-93", None],
    "22CaCeVaCaa.2105" : ["1-0-0-1-0-1-8-7-s-A2\"-93", None],
    "22CaCeVaCaa.656" : ["0-0-1-1-1-1-8-1-s-A2\"-107", None],
    "22CaCeVaCaa.4651" : ["0-0-1-1-1-1-8-1-s-A2\"-107", None],
    "22CaCeVaCaa.4233" : ["0-0-1-1-1-1-8-1-s-A2\"-107", None],
    "22CaCeVaCaa.436" : ["0-0-1-1-1-1-8-1-s-A2\"-107", None],
    "22CaCeVaCaa.2631" : ["0-0-1-1-1-1-8-1-s-A2\"-107", None],
    "22CaCeVaCaa.3910" : ["0-0-1-1-1-1-8-1-s-A2\"-107", None],
    "22CaCeVaCaa.713" : ["1-0-0-1-0-1-8-4-a-A2\"-109", None],
    "22CaCeVaCaa.4733" : ["1-0-0-1-0-1-8-4-a-A2\"-109", None],
    "22CaCeVaCaa.4323" : ["1-0-0-1-0-1-8-4-a-A2\"-109", None],
    "22CaCeVaCaa.2709" : ["1-0-0-1-0-1-8-4-a-A2\"-109", None],
    "22CaCeVaCa.2335" : ["1-0-0-1-0-1-8-4-a-A2\"-109", None],
    "22CaCeVaCaa.1747" : ["1-0-0-1-0-1-8-4-a-A2\"-109", None],
    "22CaCeVaCaa.4001" : ["1-0-0-1-0-1-8-4-a-A2\"-109", None],
    "22CaCeVaCaa.5781" : ["1-0-0-1-0-1-8-4-a-A2\"-109", None],
    "22CaCeVaCa.403" : ["1-0-0-1-0-1-8-4-a-A2\"-109", None],
    "22CaCeVaCaa.1254" : ["1-0-0-1-0-1-8-2-a-A2\"-113", None],
    "22CaCeVaCaa.5407" : ["1-0-0-1-0-1-8-2-a-A2\"-113", None],
    "22CaCeVaCaa.3471" : ["1-0-0-1-0-1-8-2-a-A2\"-113", None],
    "22CaCeVaCa.508" : ["1-0-0-1-0-1-8-7-a-A2'-85", None],
    "22CaCeVaCa.800" : ["1-0-0-1-0-1-8-7-a-A2'-85", None],
    "22CaCeVaCaa.448" : ["1-0-0-1-0-1-8-7-a-A2'-85", None],
    "22CaCeVaCaa.2254" : ["1-0-0-1-0-1-8-7-a-A2'-85", None],
    "22CaCeVaCaa.4265" : ["1-0-0-1-0-1-8-7-a-A2'-85", None],
    "22CaCeVaCaa.2112" : ["1-0-0-1-0-1-8-7-a-A2'-85", None],
    "22CaCeVaCaa.1202" : ["1-0-0-1-0-1-8-1-a-A2'-102", None],
    "22CaCeVaCaa.5368" : ["1-0-0-1-0-1-8-1-a-A2'-102", None],
    "22CaCeVaCaa.2972" : ["1-0-0-1-0-1-8-1-a-A2'-102", None],
    "22CaCeVaCaa.3414" : ["1-0-0-1-0-1-8-1-a-A2'-102", None],
    "22CaCeVaCaa.4152" : ["1-0-0-1-0-1-8-8-s-E\"-167", None],
    "22CaCeVaCaa.384" : ["1-0-0-1-0-1-8-8-s-E\"-167", None],
    "22CaCeVaCaa.2120" : ["1-0-0-1-0-1-8-8-s-E\"-167", None],
    "22CaCeVaCaa.1683" : ["0-2-0-2-0-0-8-8-a-E\"-177", None],
    "22CaCeVaCaa.165" : ["0-2-0-2-0-0-8-8-a-E\"-177", None],
    "22CaCeVaCaa.3692" : ["0-2-0-2-0-0-8-8-a-E\"-177", None],
    "22CaCeVaCaa.3505" : ["0-2-0-2-0-0-8-8-a-E\"-177", None],
    "22CaCeVaCaa.1278" : ["0-2-0-2-0-0-8-8-a-E\"-177", None],
    "22CaCeVaCaa.2249" : ["1-0-0-1-0-1-8-7-s-E\"-188", None],
    "22CaCeVaCaa.449" : ["1-0-0-1-0-1-8-7-s-E\"-188", None],
    "22CaCeVaCaa.4258" : ["1-0-0-1-0-1-8-7-s-E\"-188", None],
    "22CaCeVaCaa.4102" : ["1-0-0-1-0-1-8-7-s-E\"-188", None],
    "22CaCeVaCaa.1830" : ["1-0-0-1-0-1-8-7-s-E\"-188", None],
    "22CaCeVaCaa.4606" : ["0-2-0-2-0-2-8-6-s-E\"-199", None],
    "22CaCeVaCaa.630" : ["0-2-0-2-0-2-8-6-s-E\"-199", None],
    "22CaCeVaCaa.2583" : ["0-2-0-2-0-2-8-6-s-E\"-199", None],
    "22CaCeVaCaa.5852" : ["0-2-0-2-0-2-8-6-s-E\"-199", None],
    "22CaCeVaCaa.1857" : ["0-2-0-2-0-2-8-6-s-E\"-199", None],
    "22CaCeVaCaa.4124" : ["0-2-0-2-0-2-8-6-s-E\"-199", None],
    "22CaCeVaCaa.1684" : ["0-0-0-3-0-1-8-0-s-E'-177", None],
    "22CaCeVaCaa.164" : ["0-0-0-3-0-1-8-0-s-E'-177", None],
    "22CaCeVaCaa.3695" : ["0-0-0-3-0-1-8-0-s-E'-177", None],
    "22CaCeVaCaa.1260" : ["0-0-0-3-0-1-8-0-s-E'-177", None],
    "22CaCeVaCaa.4434" : ["1-0-0-1-0-1-8-6-s-E'-197", None],
    "22CaCeVaCaa.535" : ["1-0-0-1-0-1-8-6-s-E'-197", None],
    "22CaCeVaCaa.2413" : ["1-0-0-1-0-1-8-6-s-E'-197", None],
    "22CaCeVaCaa.5774" : ["1-0-0-1-0-1-8-6-s-E'-197", None],
    "22CaCeVaCaa.1723" : ["1-0-0-1-0-1-8-6-s-E'-197", None],
    "22CaCeVaCaa.3983" : ["1-0-0-1-0-1-8-6-s-E'-197", None],
    "22CaCeVaCaa.4588" : ["1-0-0-1-0-1-8-5-a-E'-201", None],
    "22CaCeVaCaa.615" : ["1-0-0-1-0-1-8-5-a-E'-201", None],
    "22CaCeVaCaa.2553" : ["1-0-0-1-0-1-8-5-a-E'-201", None],
    "22CaCeVaCaa.1858" : ["1-0-0-1-0-1-8-5-a-E'-201", None],
    "22CaCeVaCa.513" : ["1-0-0-1-0-1-8-5-a-E'-201", None],
    "22CaCeVaCaa.3738" : ["0-0-1-1-1-1-8-4-a-E'-212", None],
    "22CaCeVaCaa.4743" : ["0-0-1-1-1-1-8-4-a-E'-212", None],
    "22CaCeVaCaa.2725" : ["0-0-1-1-1-1-8-4-a-E'-212", None],
    "22CaCeVaCaa.1499" : ["0-0-1-1-1-1-8-4-a-E'-212", None],
    "22CaCeVaCaa.5610" : ["0-0-1-1-1-1-8-4-a-E'-212", None],
    "22CaCeVaCaa.790" : ["0-2-0-2-0-0-8-5-a-E'-215", None],
    "22CaCeVaCaa.4874" : ["0-2-0-2-0-0-8-5-a-E'-215", None],
    "22CaCeVaCaa.2862" : ["0-2-0-2-0-0-8-5-a-E'-215", None],
    "22CaCeVaCaa.5691" : ["0-2-0-2-0-0-8-5-a-E'-215", None],
    "22CaCeVaCaa.1097" : ["1-0-0-1-0-1-8-1-a-E'-224", None],
    "22CaCeVaCaa.5255" : ["1-0-0-1-0-1-8-1-a-E'-224", None],
    "22CaCeVaCaa.3289" : ["1-0-0-1-0-1-8-1-a-E'-224", None],
    "22CaCeVaCaa.3147" : ["1-0-0-1-0-1-8-1-a-E'-224", None],
    "22CaCeVaCaa.5141" : ["1-0-0-1-0-1-8-1-a-E'-224", None],
    "22CaCeVaCaa.988" : ["1-0-0-1-0-1-8-1-a-E'-224", None],
    "22CaCeVaCaa.5675" : ["1-0-0-1-0-1-8-1-a-E'-224", None],
    "22CaCeVaCaa.3847" : ["1-0-0-1-0-1-8-1-a-E'-224", None],
    "22CaCeVaCaa.340" : ["1-0-0-1-0-1-9-7-s-A2\"-100", None],
    "22CaCeVaCaa.2285" : ["1-0-0-1-0-1-9-7-s-A2\"-100", None],
    "22CaCeVaCaa.4552" : ["1-0-0-1-0-1-9-7-s-A2\"-100", None],
    "22CaCeVaCaa.4383" : ["1-0-0-1-0-1-9-7-s-A2\"-100", None],
    "22CaCeVaCaa.1865" : ["1-0-0-1-0-1-9-7-s-A2\"-100", None],
    "22CaCeVaCaa.2585" : ["1-0-0-1-0-1-9-5-s-A2\"-114", None],
    "22CaCeVaCaa.4837" : ["1-0-0-1-0-1-9-5-s-A2\"-114", None],
    "22CaCeVaCaa.1382" : ["1-0-0-1-0-1-9-5-s-A2\"-114", None],
    "22CaCeVaCaa.3860" : ["1-0-0-1-0-1-9-5-s-A2\"-114", None],
    "22CaCeVaCaa.5834" : ["1-0-0-1-0-1-9-5-s-A2\"-114", None],
    "22CaCeVaCaa.2723" : ["1-0-0-1-0-1-9-4-a-A2\"-115", None],
    "22CaCeVaCaa.4984" : ["1-0-0-1-0-1-9-4-a-A2\"-115", None],
    "22CaCeVaCaa.1522" : ["1-0-0-1-0-1-9-4-a-A2\"-115", None],
    "22CaCeVaCaa.4000" : ["1-0-0-1-0-1-9-4-a-A2\"-115", None],
    "22CaCeVaCaa.5905" : ["1-0-0-1-0-1-9-4-a-A2\"-115", None],
    "22CaCeVaCaa.573" : ["1-0-0-1-0-1-9-4-a-A2\"-115", None],
    "22CaCeVaCa.2566" : ["1-0-0-1-0-1-9-4-a-A2\"-115", None],
    "22CaCeVaCaa.3398" : ["1-0-0-1-0-1-9-1-s-A2\"-119", None],
    "22CaCeVaCaa.2966" : ["1-0-0-1-0-1-9-1-s-A2\"-119", None],
    "22CaCeVaCaa.5543" : ["1-0-0-1-0-1-9-1-s-A2\"-119", None],
    "22CaCeVaCaa.976" : ["1-0-0-1-0-1-9-1-s-A2\"-119", None],
    "22CaCeVaCaa.3423" : ["1-0-0-1-0-1-9-2-a-A2\"-120", None],
    "22CaCeVaCaa.2987" : ["1-0-0-1-0-1-9-2-a-A2\"-120", None],
    "22CaCeVaCaa.5559" : ["1-0-0-1-0-1-9-2-a-A2\"-120", None],
    "22CaCeVaCaa.995" : ["1-0-0-1-0-1-9-2-a-A2\"-120", None],
    "22CaCeVaCaa.3759" : ["0-2-1-0-1-0-9-5-s-A2\"-124", None],
    "22CaCeVaCaa.5771" : ["0-2-1-0-1-0-9-5-s-A2\"-124", None],
    "22CaCeVaCaa.1296" : ["0-2-1-0-1-0-9-5-s-A2\"-124", None],
    "22CaCeVaCaa.261" : ["1-0-0-1-0-1-9-8-s-A2'-107", None],
    "22CaCeVaCaa.2091" : ["1-0-0-1-0-1-9-8-s-A2'-107", None],
    "22CaCeVaCaa.4355" : ["1-0-0-1-0-1-9-8-s-A2'-107", None],
    "22CaCeVaCaa.4223" : ["1-0-0-1-0-1-9-8-s-A2'-107", None],
    "22CaCeVaCaa.1708" : ["1-0-0-1-0-1-9-8-s-A2'-107", None],
    "22CaCeVaCaa.3464" : ["1-0-0-1-0-1-9-2-s-A2'-130", None],
    "22CaCeVaCaa.5241" : ["1-0-0-1-0-1-9-2-s-A2'-130", None],
    "22CaCeVaCaa.5586" : ["1-0-0-1-0-1-9-2-s-A2'-130", None],
    "22CaCeVaCaa.1027" : ["1-0-0-1-0-1-9-2-s-A2'-130", None],
    "22CaCeVaCaa.1932" : ["1-0-0-1-0-1-9-9-s-E\"-187", None],
    "22CaCeVaCaa.4189" : ["1-0-0-1-0-1-9-9-s-E\"-187", None],
    "22CaCeVaCaa.1808" : ["1-0-0-1-0-1-9-9-s-E\"-187", None],
    "22CaCeVaCaa.4524" : ["1-0-0-1-0-1-9-7-s-E\"-216", None],
    "22CaCeVaCaa.333" : ["1-0-0-1-0-1-9-7-s-E\"-216", None],
    "22CaCeVaCaa.2262" : ["1-0-0-1-0-1-9-7-s-E\"-216", None],
    "22CaCeVaCaa.5958" : ["1-0-0-1-0-1-9-7-s-E\"-216", None],
    "22CaCeVaCaa.1603" : ["1-0-0-1-0-1-9-7-s-E\"-216", None],
    "22CaCeVaCaa.4101" : ["1-0-0-1-0-1-9-7-s-E\"-216", None],
    "22CaCeVaCaa.2435" : ["1-0-0-1-0-1-9-6-a-E\"-225", None],
    "22CaCeVaCaa.4703" : ["1-0-0-1-0-1-9-6-a-E\"-225", None],
    "22CaCeVaCaa.1490" : ["1-0-0-1-0-1-9-6-a-E\"-225", None],
    "22CaCeVaCaa.3970" : ["1-0-0-1-0-1-9-6-a-E\"-225", None],
    "22CaCeVaCaa.5899" : ["1-0-0-1-0-1-9-6-a-E\"-225", None],
    "22CaCeVaCaa.583" : ["1-0-0-1-0-1-9-4-s-E\"-240", None],
    "22CaCeVaCaa.5749" : ["1-0-0-1-0-1-9-4-s-E\"-240", None],
    "22CaCeVaCaa.4991" : ["1-0-0-1-0-1-9-4-s-E\"-240", None],
    "22CaCeVaCaa.2737" : ["1-0-0-1-0-1-9-4-s-E\"-240", None],
    "22CaCeVaCaa.1258" : ["1-0-0-1-0-1-9-4-s-E\"-240", None],
    "22CaCeVaCaa.3727" : ["1-0-0-1-0-1-9-4-s-E\"-240", None],
    "22CaCeVaCaa.685" : ["1-0-0-1-0-1-9-3-s-E\"-242", None],
    "22CaCeVaCaa.5856" : ["1-0-0-1-0-1-9-3-s-E\"-242", None],
    "22CaCeVaCaa.5164" : ["1-0-0-1-0-1-9-3-s-E\"-242", None],
    "22CaCeVaCaa.2933" : ["1-0-0-1-0-1-9-3-s-E\"-242", None],
    "22CaCeVaCaa.1421" : ["1-0-0-1-0-1-9-3-s-E\"-242", None],
    "22CaCeVaCaa.3905" : ["1-0-0-1-0-1-9-3-s-E\"-242", None],
    "22CaCeVaCaa.657" : ["1-0-0-1-0-1-9-3-s-E\"-245", None],
    "22CaCeVaCaa.1135" : ["1-0-0-1-0-1-9-3-s-E\"-245", None],
    "22CaCeVaCaa.2864" : ["1-0-0-1-0-1-9-3-s-E\"-245", None],
    "22CaCeVaCaa.5119" : ["1-0-0-1-0-1-9-3-s-E\"-245", None],
    "22CaCeVaCaa.3596" : ["1-0-0-1-0-1-9-3-s-E\"-245", None],
    "22CaCeVaCaa.5669" : ["1-0-0-1-0-1-9-3-s-E\"-245", None],
    "22CaCeVaCaa.278" : ["1-0-0-1-0-1-9-8-s-E'-191", None],
    "22CaCeVaCaa.2134" : ["1-0-0-1-0-1-9-8-s-E'-191", None],
    "22CaCeVaCaa.4405" : ["1-0-0-1-0-1-9-8-s-E'-191", None],
    "22CaCeVaCaa.2014" : ["1-0-0-1-0-1-9-8-s-E'-191", None],
    "22CaCeVaCaa.4534" : ["1-0-0-1-0-1-9-7-a-E'-210", None],
    "22CaCeVaCaa.2277" : ["1-0-0-1-0-1-9-7-a-E'-210", None],
    "22CaCeVaCaa.339" : ["1-0-0-1-0-1-9-7-a-E'-210", None],
    "22CaCeVaCaa.5955" : ["1-0-0-1-0-1-9-7-a-E'-210", None],
    "22CaCeVaCaa.1599" : ["1-0-0-1-0-1-9-7-a-E'-210", None],
    "22CaCeVaCaa.4090" : ["1-0-0-1-0-1-9-7-a-E'-210", None],
    "22CaCeVaCaa.571" : ["1-0-0-1-0-1-9-4-s-E'-235", None],
    "22CaCeVaCaa.5754" : ["1-0-0-1-0-1-9-4-s-E'-235", None],
    "22CaCeVaCaa.2719" : ["1-0-0-1-0-1-9-4-s-E'-235", None],
    "22CaCeVaCaa.4979" : ["1-0-0-1-0-1-9-4-s-E'-235", None],
    "22CaCeVaCaa.3728" : ["1-0-0-1-0-1-9-4-s-E'-235", None],
    "22CaCeVaCaa.1259" : ["1-0-0-1-0-1-9-4-s-E'-235", None],
    "22CaCeVaCaa.677" : ["1-0-0-1-0-1-9-3-a-E'-241", None],
    "22CaCeVaCaa.5855" : ["1-0-0-1-0-1-9-3-a-E'-241", None],
    "22CaCeVaCaa.2919" : ["1-0-0-1-0-1-9-3-a-E'-241", None],
    "22CaCeVaCaa.5148" : ["1-0-0-1-0-1-9-3-a-E'-241", None],
    "22CaCeVaCaa.3903" : ["1-0-0-1-0-1-9-3-a-E'-241", None],
    "22CaCeVaCaa.1419" : ["1-0-0-1-0-1-9-3-a-E'-241", None],
    "22CaCeVaCaa.660" : ["1-0-0-1-0-1-9-3-a-E'-244", None],
    "22CaCeVaCaa.1131" : ["1-0-0-1-0-1-9-3-a-E'-244", None],
    "22CaCeVaCaa.5128" : ["1-0-0-1-0-1-9-3-a-E'-244", None],
    "22CaCeVaCaa.3592" : ["1-0-0-1-0-1-9-3-a-E'-244", None],
    "22CaCeVaCaa.5666" : ["1-0-0-1-0-1-9-3-a-E'-244", None],
    "22CaCeVaCaa.868" : ["1-0-0-1-0-1-9-2-s-E'-248", None],
    "22CaCeVaCaa.779" : ["1-0-0-1-0-1-9-2-s-E'-248", None],
    "22CaCeVaCaa.1319" : ["1-0-0-1-0-1-9-2-s-E'-248", None],
    "22CaCeVaCaa.6001" : ["1-0-0-1-0-1-9-2-s-E'-248", None],
    "22CaCeVaCaa.3237" : ["1-0-0-1-0-1-9-2-s-E'-248", None],
    "22CaCeVaCaa.5422" : ["1-0-0-1-0-1-9-2-s-E'-248", None],
    "22CaCeVaCaa.5308" : ["1-0-0-1-0-1-9-2-s-E'-248", None],
    "22CaCeVaCaa.4198" : ["1-0-0-1-0-1-9-2-s-E'-248", None],
    "22CaCeVaCaa.3093" : ["1-0-0-1-0-1-9-2-s-E'-248", None],
    "22CaCeVaCaa.5792" : ["1-0-0-1-0-1-9-2-s-E'-248", None],
    "22CaCeVaCaa.470" : ["1-0-0-1-0-1-10-4-s-A2\"-137", None],
    "22CaCeVaCaa.5245" : ["1-0-0-1-0-1-10-4-s-A2\"-137", None],
    "22CaCeVaCaa.286" : ["1-0-0-1-0-1-10-4-s-A2\"-137", None],
    "22CaCeVaCaa.4028" : ["1-0-0-1-0-1-10-4-s-A2\"-137", None],
    "22CaCeVaCaa.1321" : ["1-0-0-1-0-1-10-4-s-A2\"-137", None],
    "22CaCeVaCaa.6030" : ["1-0-0-1-0-1-10-4-s-A2\"-137", None],
    "22CaCeVaCaa.2774" : ["1-0-0-1-0-1-10-4-s-A2\"-137", None],
    "22CaCeVaCa.2830" : ["1-0-0-1-0-1-10-4-s-A2\"-137", None],
    "22CaCeVaCaa.794" : ["1-0-0-1-0-1-10-1-s-A2\"-141", None],
    "22CaCeVaCaa.5688" : ["1-0-0-1-0-1-10-1-s-A2\"-141", None],
    "22CaCeVaCaa.5402" : ["1-0-0-1-0-1-10-1-s-A2\"-141", None],
    "22CaCeVaCaa.565" : ["1-0-0-1-0-1-10-1-s-A2\"-141", None],
    "22CaCeVaCaa.4372" : ["1-0-0-1-0-1-10-10-s-A2'-93", None],
    "22CaCeVaCaa.79" : ["1-0-0-1-0-1-10-10-s-A2'-93", None],
    "22CaCeVaCaa.1854" : ["1-0-0-1-0-1-10-10-s-A2'-93", None],
    "22CaCeVaCaa.2107" : ["1-0-0-1-0-1-10-8-s-A2'-110", None],
    "22CaCeVaCaa.147" : ["1-0-0-1-0-1-10-8-s-A2'-110", None],
    "22CaCeVaCaa.4625" : ["1-0-0-1-0-1-10-8-s-A2'-110", None],
    "22CaCeVaCaa.6114" : ["1-0-0-1-0-1-10-8-s-A2'-110", None],
    "22CaCeVaCaa.1484" : ["1-0-0-1-0-1-10-8-s-A2'-110", None],
    "22CaCeVaCaa.4221" : ["1-0-0-1-0-1-10-8-s-A2'-110", None],
    "22CaCeVaCaa.2317" : ["1-0-0-1-0-1-10-7-a-A2'-113", None],
    "22CaCeVaCaa.250" : ["1-0-0-1-0-1-10-7-a-A2'-113", None],
    "22CaCeVaCaa.4819" : ["1-0-0-1-0-1-10-7-a-A2'-113", None],
    "22CaCeVaCaa.1682" : ["1-0-0-1-0-1-10-7-a-A2'-113", None],
    "22CaCeVaCaa.4417" : ["1-0-0-1-0-1-10-7-a-A2'-113", None],
    "22CaCeVaCa.404" : ["1-0-0-1-0-1-10-7-a-A2'-113", None],
    "22CaCeVaCaa.808" : ["1-0-0-1-0-1-10-1-a-A2'-132", None],
    "22CaCeVaCaa.5705" : ["1-0-0-1-0-1-10-1-a-A2'-132", None],
    "22CaCeVaCaa.2970" : ["1-0-0-1-0-1-10-1-a-A2'-132", None],
    "22CaCeVaCaa.3406" : ["1-0-0-1-0-1-10-1-a-A2'-132", None],
    "22CaCeVaCa.117" : ["0-1-0-2-0-0-10-5-a-E\"-139", None],
    "22CaCeVaCa.1767" : ["0-1-0-2-0-0-10-5-a-E\"-139", None],
    "22CaCeVaCa.3992" : ["0-1-0-2-0-0-10-5-a-E\"-139", None],
    "21CeCaCo.195" : ["0-1-0-2-0-0-10-5-a-E\"-139", None],
    "22CaCeVaCa.1132" : ["0-1-0-2-0-0-10-5-a-E\"-139", None],
    "22CaCeVaCa.3430" : ["0-1-0-2-0-0-10-5-a-E\"-139", None],
    "22CaCeVaCa.5235" : ["0-1-0-2-0-0-10-5-a-E\"-139", None],
    "22CaCeVaCa.3321" : ["0-1-0-2-0-0-10-5-a-E\"-139", None],
    "21CeCaCo.269" : ["0-1-0-2-0-0-10-5-a-E\"-139", None],
    "22CaCeVaCaa.119" : ["1-0-0-1-0-1-10-9-s-E\"-202", None],
    "22CaCeVaCaa.2011" : ["1-0-0-1-0-1-10-9-s-E\"-202", None],
    "22CaCeVaCaa.4520" : ["1-0-0-1-0-1-10-9-s-E\"-202", None],
    "22CaCeVaCaa.1924" : ["1-0-0-1-0-1-10-9-s-E\"-202", None],
    "22CaCeVaCaa.155" : ["1-0-0-1-0-1-10-8-a-E\"-215", None],
    "22CaCeVaCaa.2128" : ["1-0-0-1-0-1-10-8-a-E\"-215", None],
    "22CaCeVaCaa.4654" : ["1-0-0-1-0-1-10-8-a-E\"-215", None],
    "22CaCeVaCaa.4531" : ["1-0-0-1-0-1-10-8-a-E\"-215", None],
    "22CaCeVaCaa.1777" : ["1-0-0-1-0-1-10-8-a-E\"-215", None],
    "22CaCeVaCaa.452" : ["1-0-0-1-0-1-10-4-a-E\"-268", None],
    "22CaCeVaCaa.5214" : ["1-0-0-1-0-1-10-4-a-E\"-268", None],
    "22CaCeVaCaa.1031" : ["1-0-0-1-0-1-10-4-a-E\"-268", None],
    "22CaCeVaCaa.3717" : ["1-0-0-1-0-1-10-4-a-E\"-268", None],
    "22CaCeVaCaa.5877" : ["1-0-0-1-0-1-10-4-a-E\"-268", None],
    "22CaCeVaCaa.572" : ["1-0-0-1-0-1-10-3-s-E\"-272", None],
    "22CaCeVaCaa.2968" : ["1-0-0-1-0-1-10-3-s-E\"-272", None],
    "22CaCeVaCaa.5420" : ["1-0-0-1-0-1-10-3-s-E\"-272", None],
    "22CaCeVaCaa.5296" : ["1-0-0-1-0-1-10-3-s-E\"-272", None],
    "22CaCeVaCaa.5991" : ["1-0-0-1-0-1-10-3-s-E\"-272", None],
    "22CaCeVaCaa.923" : ["1-0-0-1-0-1-10-3-s-E\"-273", None],
    "22CaCeVaCaa.3581" : ["1-0-0-1-0-1-10-3-s-E\"-273", None],
    "22CaCeVaCaa.518" : ["1-0-0-1-0-1-10-3-s-E\"-273", None],
    "22CaCeVaCaa.5328" : ["1-0-0-1-0-1-10-3-s-E\"-273", None],
    "22CaCeVaCaa.5804" : ["1-0-0-1-0-1-10-3-s-E\"-273", None],
    "22CaCeVaCaa.4250" : ["1-0-0-1-0-1-10-10-s-E'-203", None],
    "22CaCeVaCaa.1692" : ["1-0-0-1-0-1-10-10-s-E'-203", None],
    "22CaCeVaCa.681" : ["1-0-0-1-0-1-10-9-a-E'-205", None],
    "22CaCeVaCaa.116" : ["1-0-0-1-0-1-10-9-a-E'-205", None],
    "22CaCeVaCaa.1995" : ["1-0-0-1-0-1-10-9-a-E'-205", None],
    "22CaCeVaCaa.4502" : ["1-0-0-1-0-1-10-9-a-E'-205", None],
    "22CaCeVaCaa.1931" : ["1-0-0-1-0-1-10-9-a-E'-205", None],
    "22CaCeVaCaa.168" : ["1-0-0-1-0-1-10-8-s-E'-220", None],
    "22CaCeVaCaa.2152" : ["1-0-0-1-0-1-10-8-s-E'-220", None],
    "22CaCeVaCaa.4671" : ["1-0-0-1-0-1-10-8-s-E'-220", None],
    "22CaCeVaCaa.1265" : ["1-0-0-1-0-1-10-6-s-E'-253", None],
    "22CaCeVaCaa.3971" : ["1-0-0-1-0-1-10-6-s-E'-253", None],
    "22CaCeVaCaa.6010" : ["1-0-0-1-0-1-10-6-s-E'-253", None],
    "22CaCeVaCaa.2711" : ["1-0-0-1-0-1-10-4-s-E'-266", None],
    "22CaCeVaCaa.5204" : ["1-0-0-1-0-1-10-4-s-E'-266", None],
    "22CaCeVaCaa.5878" : ["1-0-0-1-0-1-10-4-s-E'-266", None],
    "22CaCeVaCaa.1033" : ["1-0-0-1-0-1-10-4-s-E'-266", None],
    "22CaCeVaCaa.3720" : ["1-0-0-1-0-1-10-4-s-E'-266", None],
    "22CaCeVaCaa.4876" : ["1-0-0-1-0-1-11-8-a-A2\"-122", None],
    "22CaCeVaCaa.2127" : ["1-0-0-1-0-1-11-8-a-A2\"-122", None],
    "22CaCeVaCaa.93" : ["1-0-0-1-0-1-11-8-a-A2\"-122", None],
    "22CaCeVaCaa.1252" : ["1-0-0-1-0-1-11-8-a-A2\"-122", None],
    "22CaCeVaCaa.6197" : ["1-0-0-1-0-1-11-8-a-A2\"-122", None],
    "22CaCeVaCaa.4207" : ["1-0-0-1-0-1-11-8-a-A2\"-122", None],
    "22CaCeVaCaa.6048" : ["1-0-0-1-0-1-11-5-s-A2\"-140", None],
    "22CaCeVaCaa.3826" : ["1-0-0-1-0-1-11-5-s-A2\"-140", None],
    "22CaCeVaCaa.5288" : ["1-0-0-1-0-1-11-5-s-A2\"-140", None],
    "22CaCeVaCaa.937" : ["1-0-0-1-0-1-11-5-s-A2\"-140", None],
    "22CaCeVaCaa.28" : ["1-0-0-1-0-1-11-10-s-A2'-113", None],
    "22CaCeVaCaa.1871" : ["1-0-0-1-0-1-11-10-s-A2'-113", None],
    "22CaCeVaCaa.4644" : ["1-0-0-1-0-1-11-10-s-A2'-113", None],
    "22CaCeVaCaa.2584" : ["1-0-0-1-0-1-11-5-a-A2'-150", None],
    "22CaCeVaCaa.5297" : ["1-0-0-1-0-1-11-5-a-A2'-150", None],
    "22CaCeVaCaa.6045" : ["1-0-0-1-0-1-11-5-a-A2'-150", None],
    "22CaCeVaCaa.3822" : ["1-0-0-1-0-1-11-5-a-A2'-150", None],
    "22CaCeVaCaa.933" : ["1-0-0-1-0-1-11-5-a-A2'-150", None],
    "22CaCeVaCaa.4485" : ["1-0-0-1-0-1-11-11-s-E\"-213", None],
    "22CaCeVaCaa.1722" : ["1-0-0-1-0-1-11-11-s-E\"-213", None],
    "22CaCeVaCaa.63" : ["1-0-0-1-0-1-11-9-s-E\"-231", None],
    "22CaCeVaCaa.2026" : ["1-0-0-1-0-1-11-9-s-E\"-231", None],
    "22CaCeVaCaa.4776" : ["1-0-0-1-0-1-11-9-s-E\"-231", None],
    "22CaCeVaCaa.1687" : ["1-0-0-1-0-1-11-9-s-E\"-231", None],
    "22CaCeVaCa.470" : ["1-0-0-1-0-1-11-9-a-E'-230", None],
    "22CaCeVaCaa.58" : ["1-0-0-1-0-1-11-9-a-E'-230", None],
    "22CaCeVaCaa.2007" : ["1-0-0-1-0-1-11-9-a-E'-230", None],
    "22CaCeVaCaa.4754" : ["1-0-0-1-0-1-11-9-a-E'-230", None],
    "22CaCeVaCaa.4685" : ["1-0-0-1-0-1-11-9-a-E'-230", None],
    "22CaCeVaCaa.2173" : ["1-0-0-1-0-1-11-8-s-E'-246", None],
    "22CaCeVaCaa.100" : ["1-0-0-1-0-1-11-8-s-E'-246", None],
    "22CaCeVaCaa.4922" : ["1-0-0-1-0-1-11-8-s-E'-246", None],
    "22CaCeVaCaa.1552" : ["1-0-0-1-0-1-11-8-s-E'-246", None],
    "22CaCeVaCaa.4618" : ["1-0-0-1-0-1-12-11-s-A2\"-127", None],
    "22CaCeVaCaa.4573" : ["1-0-0-1-0-1-12-11-s-A2\"-127", None],
    "22CaCeVaCaa.1334" : ["1-0-0-1-0-1-12-11-s-A2\"-127", None],
    "22CaCeVaCaa.1579" : ["1-0-0-1-0-1-12-12-a-E\"-224", None],
    "22CaCeVaCaa.4577" : ["1-0-0-1-0-1-12-12-a-E\"-224", None],
    "22CaCeVaCaa.4697" : ["1-0-0-1-0-1-13-13-s-A2\"-115", None],
    "22CaCeVaCaa.1453" : ["1-0-0-1-0-1-13-13-s-A2\"-115", None],
       "23CaCeVo.3285" : ["0-1-1-1-1-1-7-3-a-E'-282", None],   # Reassigned for consistency with other Cacciani/Huang lines
       "23CaCeVo.1658" : ["0-1-1-1-1-1-7-3-a-E'-282", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.846" : ["0-1-1-1-1-1-7-3-a-E'-282", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.3285" : ["0-1-1-1-1-1-7-3-a-E'-282", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.1658" : ["0-1-1-1-1-1-7-3-a-E'-282", None],   # Reassigned for consistency with other Cacciani/Huang lines
         "23CaCeVo.846" : ["0-1-1-1-1-1-7-3-a-E'-282", None],   # Reassigned for consistency with other Cacciani/Huang lines
     "22CaCeVaCaa.4499" : ["0-1-1-1-1-1-8-8-s-E'-285", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.2820" : ["0-1-1-1-1-1-8-8-s-E'-285", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.2821" : ["0-1-1-1-1-1-8-8-s-E'-285", None],   # Reassigned for consistency with other Cacciani/Huang lines
     "22CaCeVaCaa.4500" : ["0-1-1-1-1-1-8-8-s-E'-285", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.2945" : ["0-1-1-1-1-1-8-8-s-E'-285", None],   # Reassigned for consistency with other Cacciani/Huang lines
       "21CaCeBeCa.608" : ["0-1-1-1-1-1-8-8-s-E'-285", None],   # Reassigned for consistency with other Cacciani/Huang lines
      "21CaCeBeCa.1156" : ["0-1-1-1-1-1-8-8-s-E'-285", None],   # Reassigned for consistency with other Cacciani/Huang lines
      "21CaCeBeCa.1157" : ["0-1-1-1-1-1-8-8-s-E'-285", None],   # Reassigned for consistency with other Cacciani/Huang lines
         "22HuSuTo.264" : ["0-1-1-1-1-1-8-8-s-E'-285", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.1730" : ["0-1-1-1-1-1-8-8-s-E'-285", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.1731" : ["0-1-1-1-1-1-8-8-s-E'-285", None],   # Reassigned for consistency with other Cacciani/Huang lines
         "23CaCeVo.161" : ["0-1-1-1-1-1-8-8-s-E'-285", None],   # Reassigned for consistency with other Cacciani/Huang lines
         "23CaCeVo.160" : ["0-1-1-1-1-1-8-8-s-E'-285", None],   # Reassigned for consistency with other Cacciani/Huang lines
         "23CaCeVo.945" : ["0-1-1-1-1-1-8-8-s-E'-285", None],   # Reassigned for consistency with other Cacciani/Huang lines
         "23CaCeVo.946" : ["0-1-1-1-1-1-8-8-s-E'-285", None],   # Reassigned for consistency with other Cacciani/Huang lines
         "22HuSuTo.263" : ["0-1-1-1-1-1-8-8-s-E'-285", None],   # Reassigned for consistency with other Cacciani/Huang lines
       "21CaCeBeCa.234" : ["0-1-1-1-1-1-8-2-s-E'-324", None],   # Reassigned for consistency with other Cacciani/Huang lines
      "21CaCeBeCa.1371" : ["0-1-1-1-1-1-8-2-s-E'-324", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.1306" : ["0-1-1-1-1-1-8-2-s-E'-324", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.2029" : ["0-1-1-1-1-1-8-2-s-E'-324", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.1307" : ["0-1-1-1-1-1-8-2-s-E'-324", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.2030" : ["0-1-1-1-1-1-8-2-s-E'-324", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.2713" : ["0-1-1-1-1-1-8-2-s-E'-324", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.3294" : ["0-1-1-1-1-1-8-2-s-E'-324", None],   # Reassigned for consistency with other Cacciani/Huang lines
         "23CaCeVo.405" : ["0-1-1-1-1-1-8-2-s-E'-324", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.3295" : ["0-1-1-1-1-1-8-2-s-E'-324", None],   # Reassigned for consistency with other Cacciani/Huang lines
         "22HuSuTo.248" : ["0-1-1-1-1-1-8-2-s-E'-324", None],   # Reassigned for consistency with other Cacciani/Huang lines
         "22HuSuTo.249" : ["0-1-1-1-1-1-8-2-s-E'-324", None],   # Reassigned for consistency with other Cacciani/Huang lines
       "21CaCeBeCa.861" : ["0-1-1-1-1-1-8-2-s-E'-324", None],   # Reassigned for consistency with other Cacciani/Huang lines
     "22CaCeVaCaa.3290" : ["0-1-1-1-1-1-8-2-s-E'-324", None],   # Reassigned for consistency with other Cacciani/Huang lines
     "22CaCeVaCaa.5277" : ["0-1-1-1-1-1-8-2-s-E'-324", None],   # Reassigned for consistency with other Cacciani/Huang lines
         "22HuSuTo.247" : ["0-1-1-1-1-1-8-2-s-E'-324", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.2947" : ["0-1-1-1-1-1-9-7-a-A2'-176", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.1957" : ["0-1-1-1-1-1-9-7-a-A2'-176", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.2670" : ["0-1-1-1-1-1-9-7-a-A2'-176", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.1122" : ["0-1-1-1-1-1-9-7-a-A2'-176", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.1956" : ["0-1-1-1-1-1-9-7-a-A2'-176", None],   # Reassigned for consistency with other Cacciani/Huang lines
         "22HuSuTo.402" : ["0-1-1-1-1-1-9-7-a-A2'-176", None],   # Reassigned for consistency with other Cacciani/Huang lines
      "21CaCeBeCa.1319" : ["0-1-1-1-1-1-9-7-a-A2'-176", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.2870" : ["0-3-0-2-0-0-9-9-a-A2'-177", None],   # Reassigned for consistency with other Cacciani/Huang lines
     "22CaCeVaCaa.2478" : ["0-3-0-2-0-0-9-9-a-A2'-177", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.3242" : ["0-3-0-2-0-0-9-9-a-A2'-177", None],   # Reassigned for consistency with other Cacciani/Huang lines
         "22HuSuTo.401" : ["0-3-0-2-0-0-9-9-a-A2'-177", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.2871" : ["0-3-0-2-0-0-9-9-a-A2'-177", None],   # Reassigned for consistency with other Cacciani/Huang lines
     "22CaCeVaCaa.4740" : ["0-3-0-2-0-0-9-9-a-A2'-177", None],   # Reassigned for consistency with other Cacciani/Huang lines
       "21CaCeBeCa.736" : ["0-3-0-2-0-0-9-9-a-A2'-177", None],   # Reassigned for consistency with other Cacciani/Huang lines
    "89UrTuRaGu_S2.311" : ["0-1-1-0-1-0-9-9-s-E\"-126", None],   # Reassigned block number to correct energy ordering
       "14CeHoVeCa.235" : ["0-1-1-0-1-0-9-9-s-E\"-126", None],   # Reassigned block number to correct energy ordering
      "22CaCeVaCa.5357" : ["0-1-1-0-1-0-9-9-s-E\"-126", None],   # Reassigned block number to correct energy ordering
      "22CaCeVaCa.1588" : ["0-1-1-0-1-0-9-9-s-E\"-126", None],   # Reassigned block number to correct energy ordering
      "22CaCeVaCa.3822" : ["0-1-1-0-1-0-9-9-s-E\"-126", None],   # Reassigned block number to correct energy ordering
       "89UrTuRaGu.347" : ["0-1-1-0-1-0-9-9-s-E\"-126", None],   # Reassigned block number to correct energy ordering
         "21CeCaCo.571" : ["0-1-1-0-1-0-9-9-s-E\"-126", None],   # Reassigned block number to correct energy ordering
       "89UrTuRaGu.387" : ["0-1-1-0-1-0-9-9-s-E\"-126", None],   # Reassigned block number to correct energy ordering
         "85UrMiRa.273" : ["0-1-1-0-1-0-9-9-s-E\"-126", None],   # Reassigned block number to correct energy ordering
         "85UrMiRa.103" : ["1-1-0-0-0-0-9-7-s-E\"-127", None],   # Reassigned block number to correct energy ordering
       "85AnFiFrIl.429" : ["1-1-0-0-0-0-9-7-s-E\"-127", None],   # Reassigned block number to correct energy ordering
      "22CaCeVaCa.1138" : ["1-1-0-0-0-0-9-7-s-E\"-127", None],   # Reassigned block number to correct energy ordering
      "22CaCeVaCa.3162" : ["1-1-0-0-0-0-9-7-s-E\"-127", None],   # Reassigned block number to correct energy ordering
      "22CaCeVaCa.4926" : ["1-1-0-0-0-0-9-7-s-E\"-127", None],   # Reassigned block number to correct energy ordering
        "89UrTuRaGu.48" : ["1-1-0-0-0-0-9-7-s-E\"-127", None],   # Reassigned block number to correct energy ordering
       "89UrTuRaGu.135" : ["1-1-0-0-0-0-9-7-s-E\"-127", None],   # Reassigned block number to correct energy ordering
      "22CaCeVaCa.3066" : ["1-1-0-0-0-0-9-7-s-E\"-127", None],   # Reassigned block number to correct energy ordering
          "21CeCaCo.76" : ["1-1-0-0-0-0-9-7-s-E\"-127", None],   # Reassigned block number to correct energy ordering
           "21CeCaCo.1" : ["1-1-0-0-0-0-9-7-s-E\"-127", None],   # Reassigned block number to correct energy ordering
     "89UrTuRaGu_S2.93" : ["1-1-0-0-0-0-9-7-s-E\"-127", None],   # Reassigned block number to correct energy ordering
        "14CeHoVeCa.36" : ["1-1-0-0-0-0-9-7-s-E\"-127", None],   # Reassigned block number to correct energy ordering
        #  "22HuSuTo.287" : ["0-1-1-1-1-1-10-6-s-E'-282", None],   # Reassigned for consistency with other Cacciani/Huang lines
       "21CaCeBeCa.715" : ["0-1-1-1-1-1-10-6-s-E'-382", None],   # Reassigned for consistency with other Cacciani/Huang lines
        #  "22HuSuTo.288" : ["0-1-1-1-1-1-10-6-s-E'-282", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.2008" : ["0-1-1-1-1-1-10-6-s-E'-382", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.2652" : ["0-1-1-1-1-1-10-6-s-E'-382", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.1095" : ["0-1-1-1-1-1-10-6-s-E'-382", None],   # Reassigned for consistency with other Cacciani/Huang lines
      "21CaCeBeCa.1358" : ["0-1-1-1-1-1-10-6-s-E'-382", None],   # Reassigned for consistency with other Cacciani/Huang lines
         "23CaCeVo.690" : ["0-1-1-1-1-1-10-6-s-E'-382", None],   # Reassigned for consistency with other Cacciani/Huang lines
        "23CaCeVo.3190" : ["0-1-1-1-1-1-10-6-s-E'-382", None],   # Reassigned for consistency with other Cacciani/Huang lines
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