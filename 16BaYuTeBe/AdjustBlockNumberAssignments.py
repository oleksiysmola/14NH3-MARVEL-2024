import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)
pd.set_option('display.precision', 10)
def writeDataFrameToFile(dataFrame, fileName, header=True):
    dataFrame = dataFrame.to_string(index=False, header=header)
    with open(fileName, "w+") as fileToWriteTo:
        fileToWriteTo.write(dataFrame)
        
transitionsColumns = ["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "i'", "Gtot'", "Nb'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "i\"", "Gtot\"", "Nb\"", "Source"]

transitions = pd.read_csv("16BaYuTeBe-MARVEL-Prelim.txt", delim_whitespace=True, names=transitionsColumns)

blockNumbersToChange = {
    "16BaYuTeBe.1518":   1,
    "16BaYuTeBe.1319":  -1,
    "16BaYuTeBe.1934":  -2, 
    "16BaYuTeBe.2036":  -1, 
    "16BaYuTeBe.1535":  -1,
    "16BaYuTeBe.1266":  -1,
    "16BaYuTeBe.44":    -2,
    "16BaYuTeBe.142":   -2,
    "16BaYuTeBe.1837":  -1,
    "16BaYuTeBe.2048":  -1,
    "16BaYuTeBe.2117":  -1,
    "16BaYuTeBe.449":   -1,
    "16BaYuTeBe.340":   -1,
    "16BaYuTeBe.1860":   1,
    "16BaYuTeBe.1009":  -1,
    "16BaYuTeBe.1904":  -1,
    "16BaYuTeBe.1605":  -1,
    "16BaYuTeBe.1747":  -2,
    "16BaYuTeBe.2110":  -2,
    "16BaYuTeBe.1755":  -1,
    "16BaYuTeBe.2034":  -1,
    "16BaYuTeBe.1790":  -2,
    "16BaYuTeBe.2122":  -2,
    "16BaYuTeBe.1854":  -3,
    "16BaYuTeBe.2151":  -1,
    "16BaYuTeBe.1888":  -1,
    "16BaYuTeBe.1858":  -1,
    "16BaYuTeBe.1123":  -1,
    "16BaYuTeBe.1500":  -1,
    "16BaYuTeBe.1038":  -1,
    "16BaYuTeBe.971":   -1,
    "16BaYuTeBe.1204":   1,
    "16BaYuTeBe.968":  1,
    "16BaYuTeBe.1592": -2,
    "16BaYuTeBe.917": -1,
    "16BaYuTeBe.1983": -2,
    "16BaYuTeBe.1268": 2,
    "16BaYuTeBe.1202": -2,
    "16BaYuTeBe.944": -1,
    "16BaYuTeBe.1673": -1,
    "16BaYuTeBe.2060": 1,
    "16BaYuTeBe.1006": -1,
    "16BaYuTeBe.1494": -1,
    "16BaYuTeBe.1878": 1,
    "16BaYuTeBe.1482": 1,
    "16BaYuTeBe.1626": 1,
    "16BaYuTeBe.75": 1,
    "16BaYuTeBe.291": 1,
    "16BaYuTeBe.1036": 1,
    "16BaYuTeBe.1610": 1,
    "16BaYuTeBe.1275": -1,
    "16BaYuTeBe.1771": -1,
    "16BaYuTeBe.543": -1,
    "16BaYuTeBe.208": 1,
    "16BaYuTeBe.30": 1,
    "16BaYuTeBe.27": 1,
    "16BaYuTeBe.1446": 1,
    "16BaYuTeBe.1138": 1,
    "16BaYuTeBe.2130": -1,
    "16BaYuTeBe.1777": 1,
    "16BaYuTeBe.2031": 1,
    "16BaYuTeBe.603": 1,
    "16BaYuTeBe.1865": 1,
    "16BaYuTeBe.1621": 1,
    "16BaYuTeBe.1289": 1,
    "16BaYuTeBe.848": 1,
    "16BaYuTeBe.1975": 1,
    "16BaYuTeBe.1411": 1,
    "16BaYuTeBe.1726": 1,
    "16BaYuTeBe.1442": -1,
    "16BaYuTeBe.2062": 1,
    "16BaYuTeBe.263": -1,
    "16BaYuTeBe.1591": 1,
    "16BaYuTeBe.1929": 1,
    "16BaYuTeBe.1784": 1,
    "16BaYuTeBe.1267": 1,
    "16BaYuTeBe.1949": 1,
    "16BaYuTeBe.1371": 1,
    "16BaYuTeBe.124": -1,
    "16BaYuTeBe.1479": 1,
    "16BaYuTeBe.415": 1,
    "16BaYuTeBe.1467": 1,
    "16BaYuTeBe.935": -1,
    "16BaYuTeBe.936": 1,
    "16BaYuTeBe.1881": -1,
}

for i in range(len(transitions)):
    if transitions["Source"][i] in blockNumbersToChange.keys():
        transitions["Nb'"][i] +=  blockNumbersToChange[transitions["Source"][i]]
        
writeDataFrameToFile(transitions, "16BaYuTeBe-MARVEL.txt", header=False)