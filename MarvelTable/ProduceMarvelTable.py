import pandas as pd
from pandarallel import pandarallel
import math
import numpy as np
pandarallel.initialize(progress_bar=True)



transitionsColumns = ["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "inv'", "Gamma'", "Nb'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "inv\"", "Gamma\"", "Nb\"", "Source"]

allTransitions = pd.read_csv("../Marvel-14NH3-Main.txt", delim_whitespace=True, names=transitionsColumns)

def trimSourceTag(row):
    row["Source"] = row["Source"].split(".")[0]
    return row

allTransitions = allTransitions.parallel_apply(lambda x:trimSourceTag(x), result_type="expand", axis=1)

allTransitions = allTransitions.groupby(["Source"])

def findMarvelStatistics(dataFrame):
    dataFrame["MU"] = dataFrame[dataFrame["nu"] >= 0]["unc1"].median()
    dataFrame["AU"] = dataFrame[dataFrame["nu"] >= 0]["unc1"].mean()
    dataFrame["nuMax"] = dataFrame["nu"].max()
    dataFrame["nuMin"] = abs(dataFrame["nu"]).min()
    numberOfValidatedLines = len(dataFrame[dataFrame["nu"] >= 0])
    dataFrame["Validated"] = numberOfValidatedLines
    totalLines = len(dataFrame)
    dataFrame["TotalLines"] = totalLines
    return dataFrame

allTransitions = allTransitions.parallel_apply(lambda x:findMarvelStatistics(x))
allTransitions = allTransitions[["Source", "nuMin", "nuMax", "Validated", "TotalLines", "AU", "MU"]]
allTransitions = allTransitions.drop_duplicates()
allTransitions = allTransitions.sort_values(by="AU")
print(allTransitions.to_string(header=False))
# allTransitions = allTransitions.reset_index()
print(allTransitions["AU"]["09CaDoPu"])

# latexString = "\\begin{table}"
# latexString += "\n"
latexString = "\\begin{longtable}{c c c c c}"
latexString += "\n"
latexString += "\\hline"
latexString += "\n"
latexString += "\\endhead"
latexString += "\n"
latexString += "Segment tag & Range (cm$^{-1}$) & V/T & AU & MU \\\\"
latexString += "\n"
latexString += "\\hline"

for source in allTransitions["Source"]:
    minimumFrequency = allTransitions["nuMin"][source].squeeze()
    maximumFrequency = allTransitions["nuMax"][source].squeeze()
    validatedTransitions = allTransitions["Validated"][source].squeeze()
    totalTransitions = allTransitions["TotalLines"][source].squeeze()
    averageUncertainty = "{0:1.3e}".format(round(allTransitions["AU"][source].squeeze(), 4-math.ceil(np.log10(allTransitions["AU"][source].squeeze()))))
    medianUncertainty = "{0:1.3e}".format(round(allTransitions["MU"][source].squeeze(), 4-math.ceil(np.log10(allTransitions["MU"][source].squeeze()))))
    latexString += "\n"
    latexString += f"{source} "
    latexString += "\cite{" + source + ".NH3} &"
    latexString += f" {minimumFrequency}-{maximumFrequency} &"
    latexString += f" {validatedTransitions}/{totalTransitions} &"
    latexString += f" {averageUncertainty} & {medianUncertainty}"
    latexString += " \\\\"

latexString += "\n"
latexString += "\\hline"
latexString += "\n"
latexString += "\\caption{}"
latexString += "\n"
latexString += "\\end{longtable}"
# latexString += "\n"
# latexString += "\\caption{}"
# latexString += "\n"
# latexString += "\\end{table}"


tableFile = "MarvelTable.tex"
with open(tableFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(latexString)