import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

pd.set_option('display.float_format', '{:.6f}'.format)

marvelColumns = ["nu1", "nu2", "nu3", "nu4", "L3", "L4", "J", "K", "inv", "Gamma", "Nb", "Em", "Uncertainty", "Transitions"]

# marvelEnergies = pd.read_csv("14NH3-NewEnergies-MoreNbAssignments.txt", delim_whitespace=True, names=marvelColumns, dtype=str)
marvelEnergies = pd.read_csv("14NH3-NewEnergies.txt", delim_whitespace=True, names=marvelColumns, dtype=str)
marvelEnergiesOld = pd.read_csv("../CombinationDifferencesTests/14NH3-MarvelEnergies-2020.txt", delim_whitespace=True, names=marvelColumns, dtype=str)
# marvelEnergies = marvelEnergies[marvelEnergies["Transitions"].astype(int) > 1]

symmetryMap = {
    "A1'": "1",
    "A2'": "2",
    "E'": "3",
    "A1\"": "4",
    "A2\"": "5",
    "E\"": "6"
}

inversionMap = {
    "s": 0,
    "a": 1,
}

marvelEnergies["Gamma"] = marvelEnergies["Gamma"].map(symmetryMap)
marvelEnergies["inv"] = marvelEnergies["inv"].map(inversionMap)

marvelEnergiesOld["Gamma"] = marvelEnergiesOld["Gamma"].map(symmetryMap)
marvelEnergiesOld["inv"] = marvelEnergiesOld["inv"].map(inversionMap)

statesFileColumns = ["i", "E", "g", "J", "weight", "p", "Gamma", "Nb", "n1", "n2", "n3", "n4", "l3", "l4", "inversion", "J'", "K'", "pRot", "v1", "v2", "v3", "v4", "v5", "v6", "GammaVib", "Calc"]
states = pd.read_csv("../14N-1H3__CoYuTe.states", delim_whitespace=True, names=statesFileColumns, dtype=str)

def generateTagColumn(dataFrame):
    dataFrame["Tag"] = dataFrame["J"] + "-" + dataFrame["Gamma"] + "-" + dataFrame["Nb"]
    return dataFrame

marvelEnergies = generateTagColumn(marvelEnergies)
marvelEnergiesOld = generateTagColumn(marvelEnergiesOld)
marvelTagAlreadyPresent = []
for marvelTag in marvelEnergies["Tag"]:
    if marvelTag in marvelEnergiesOld["Tag"].to_list():
        marvelTagAlreadyPresent += [True]
    else:
        marvelTagAlreadyPresent += [False]
states = generateTagColumn(states)

columnsToConvertToInteger = ["J", "n1", "n2", "n3", "n4", "inversion"]
for column in columnsToConvertToInteger:
    states[column] = states[column].astype(int)

# states = pd.merge(states, marvelEnergies.drop(["J", "Gamma", "Nb"], axis=1), on="Tag", how="left")
marvelEnergies = pd.merge(marvelEnergies, states[["E", "Tag"]], on="Tag", how="left")

columnsToConvertToFloat = ["Em", "E"]
for column in columnsToConvertToFloat:
    marvelEnergies[column] = marvelEnergies[column].astype(float)



marvelEnergies["Obs-Calc"] = marvelEnergies["Em"] - marvelEnergies["E"]

marvelEnergies["Old"] = marvelTagAlreadyPresent

marvelEnergies = marvelEnergies.to_string(index=False)
marvelEnergiesFile = "14N-1H3__MarvelAgainstStates.energies"
with open(marvelEnergiesFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(marvelEnergies)
print("New states file ready!")

print(marvelEnergiesOld.head(20).to_string(index=False))