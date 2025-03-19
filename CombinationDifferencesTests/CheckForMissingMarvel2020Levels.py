import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)
import matplotlib.pyplot as plt
import scienceplots
plt.style.use('science')

marvelColumns = ["nu1", "nu2", "nu3", "nu4", "L3", "L4", "J", "K", "inv", "Gamma", "Nb", "E", "Uncertainty", "Transitions"]

marvelEnergies2020 = pd.read_csv("14NH3-MarvelEnergies-2020.txt", delim_whitespace=True, names=marvelColumns)
marvelEnergiesNew = pd.read_csv("14NH3-MarvelEnergies-2024.txt", delim_whitespace=True, names=marvelColumns)

def assignMarvelTags(row):
    row["Tag"] = str(row["J"]) + "-" + str(row["Gamma"]) + "-" + str(row["Nb"])
    return row

marvelEnergies2020 = marvelEnergies2020.parallel_apply(lambda x:assignMarvelTags(x), result_type="expand", axis=1)
marvelEnergiesNew = marvelEnergiesNew.parallel_apply(lambda x:assignMarvelTags(x), result_type="expand", axis=1)

def checkIf2020LevelIsPresent(row, marvelEnergiesNew):
    if row["Tag"] in marvelEnergiesNew["Tag"].tolist():
        row["Present"] = True
    else:
        row["Present"] = False
    return row

def checkIfEnergyIsNew(row, marvelEnergiesOld):
    if row["Tag"] in marvelEnergiesOld["Tag"].tolist():
        row["New"] = False
    else:
        row["New"] = True
    return row
marvelEnergies2020 = marvelEnergies2020.parallel_apply(lambda x:checkIf2020LevelIsPresent(x, marvelEnergiesNew), result_type="expand", axis=1)

missing2020Levels = marvelEnergies2020[marvelEnergies2020["Present"] == False]
print(len(missing2020Levels))
print(missing2020Levels.to_string(index=False))

marvelEnergiesNew = marvelEnergiesNew.parallel_apply(lambda x:checkIfEnergyIsNew(x, marvelEnergies2020), result_type="expand", axis=1)
marvelEnergiesOld = marvelEnergiesNew[marvelEnergiesNew["New"] == False]
marvelEnergiesNew = marvelEnergiesNew[marvelEnergiesNew["New"]]

fontsize=30
# plt.plot(marvelEnergiesNew["J"], marvelEnergiesNew["E"], "b.", label="This work")
# plt.plot(marvelEnergiesOld["J"], marvelEnergiesOld["E"], "r.", label="20FuCoTe")
# plt.xlim(0, 31)
# plt.ylim(0, 17999)
# plt.xticks(fontsize=fontsize)
# plt.yticks(fontsize=fontsize)
# plt.xlabel("J", fontsize=fontsize)
# plt.ylabel("E, cm$^{-1}$", fontsize=fontsize)
# plt.legend(prop={"size": 30}, frameon=True)
# plt.show()

# marvelEnergiesNewOneTransition = marvelEnergiesNew[marvelEnergiesNew["Transitions"] == 1]
# plt.plot(marvelEnergiesNew["E"], marvelEnergiesNew["Uncertainty"], "b.")
# plt.plot(marvelEnergiesNewOneTransition["E"], marvelEnergiesNewOneTransition["Uncertainty"], "r.")
# plt.xlim(3700, 19000)
# plt.ylim(0, 0.05)
# plt.xticks(fontsize=fontsize)
# plt.yticks(fontsize=fontsize)
# plt.xlabel("E, cm$^{-1}$", fontsize=fontsize)
# plt.ylabel("Uncertainty, cm$^{-1}$", fontsize=fontsize)
# # plt.legend(prop={"size": 30}, frameon=True)
# plt.show()

symmetryMap = {
    "A1'": "1",
    "A2'": "2",
    "E'": "3",
    "A1\"": "4",
    "A2\"": "5",
    "E\"": "6"
}

marvelEnergiesNew["Gamma"] = marvelEnergiesNew["Gamma"].map(symmetryMap)
marvelEnergiesNew = marvelEnergiesNew.parallel_apply(lambda x:assignMarvelTags(x), result_type="expand", axis=1)

statesFileColumns = ["i", "Energy", "g", "J", "weight", "p", "Gamma", "Nb", "nu1", "nu2", "nu3", "nu4", "L3", "L4", "inv", "J'", "K'", "pRot", "v1", "v2", "v3", "v4", "v5", "v6", "GammaVib", "Calc"]
states = pd.read_csv("../14N-1H3__CoYuTe.states", delim_whitespace=True, names=statesFileColumns)
states = states[states["Energy"] < 19000]
states = states[states["g"] > 0]
# states = states[states["J"] <= 8] 

states = states.parallel_apply(lambda x:assignMarvelTags(x), result_type="expand", axis=1)
print(states.head(20).to_string(header=False))
marvelEnergiesNew = marvelEnergiesNew.drop("New", axis=1)
marvelEnergiesNew = pd.merge(marvelEnergiesNew, states[["Tag", "Energy"]], on="Tag", how="left")
marvelEnergiesNew["Obs-Calc"] = abs(marvelEnergiesNew["E"] - marvelEnergiesNew["Energy"])

marvelEnergiesNewOneTransition = marvelEnergiesNew[marvelEnergiesNew["Transitions"] == 1]
plt.plot(marvelEnergiesNew["E"], marvelEnergiesNew["Obs-Calc"], "b.")
plt.plot(marvelEnergiesNewOneTransition["E"], marvelEnergiesNewOneTransition["Obs-Calc"], "r.")
# plt.xlim(3700, 19000)
# plt.ylim(0, 0.05)
plt.yscale("log")
plt.xticks(fontsize=fontsize)
plt.yticks(fontsize=fontsize)
plt.xlabel("E, cm$^{-1}$", fontsize=fontsize)
plt.ylabel(r"$|$Obs-Calc$|$, cm$^{-1}$", fontsize=fontsize)
# plt.legend(prop={"size": 30}, frameon=True)
plt.show()
marvelEnergiesNew = marvelEnergiesNew.to_string(index=False, header=False)
newEnergiesFile = "MARVEL-NewEnergiesComparison-2024.states"
with open(newEnergiesFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(marvelEnergiesNew)