import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)
pd.set_option('display.precision', 10)
def writeDataFrameToFile(dataFrame, fileName, header=True):
    dataFrame = dataFrame.to_string(index=False, header=header)
    with open(fileName, "w+") as fileToWriteTo:
        fileToWriteTo.write(dataFrame)
        
transitionsColumns = ["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "i'", "Gtot'", "Nb'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "i\"", "Gtot\"", "Nb\"", "Source", "E'", "Matched"]

transitions = pd.read_csv("86CoLe-MARVEL-Old.txt", delim_whitespace=True, names=transitionsColumns)

blockNumbersToChange = {
    "86CoLe.28": 1,
    "86CoLe.29": 1,
    "86CoLe.4": 1,
    "86CoLe.30": 1,
    "86CoLe.5": 1,
    "86CoLe.36": -1,
    "86CoLe.13": -1,
    "86CoLe.51": -2,
    "86CoLe.17": -2,
    "86CoLe.53": -1,
    "86CoLe.35": -4,
    "86CoLe.12": -4,
    "86CoLe.14": -3,
    "86CoLe.37": -3,
    "86CoLe.86": -6,
    "86CoLe.55": -5,
    "86CoLe.85": -6,
    "86CoLe.54": -6,
    "86CoLe.20": -5,
    "86CoLe.73": -3,
    "86CoLe.24": -2,
    "86CoLe.141": -11,
    "86CoLe.182": -11,
    "86CoLe.59": -11,
    "86CoLe.183": -10,
    "86CoLe.145": -5,
    "86CoLe.92": -5,
    "86CoLe.63": -5,
    "86CoLe.133": -5,
    "86CoLe.134": -5,
    "86CoLe.78": -4,
    "86CoLe.48": 1,
    "86CoLe.47": -2,
    "86CoLe.96": -2,
    "86CoLe.148": -2,
    "86CoLe.119": -5,
    "86CoLe.215": 1,
    "86CoLe.169": 1,
    "86CoLe.128": -1,
    "86CoLe.186": -2,
    "86CoLe.187": -1,
    "86CoLe.229": -1,
    "86CoLe.240": 1,
    "86CoLe.100": 1,
    "86CoLe.175": -1,
    "86CoLe.220": -1,
    "86CoLe.130": -4,
    "86CoLe.253": -3,
    "86CoLe.242": -2,
    "86CoLe.243": -4,
    "86CoLe.291": -1,
    "86CoLe.301": -1,
    "86CoLe.274": 1,
    "86CoLe.311": 1,
    "86CoLe.319": -1,
    
}

for i in range(len(transitions)):
    if transitions["Source"][i] in blockNumbersToChange.keys():
        if transitions["Matched"][i] == False:
            transitions["Nb'"][i] +=  blockNumbersToChange[transitions["Source"][i]]
            
# transitions = transitions.sort_values(by="nu")
transitions = transitions[["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "i'", "Gtot'", "Nb'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "i\"", "Gtot\"", "Nb\"", "Source"]]
writeDataFrameToFile(transitions, "86CoLe-MARVEL.txt", header=False)