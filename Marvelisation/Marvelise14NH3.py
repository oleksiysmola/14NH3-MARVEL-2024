import pandas as pd
from math import isnan
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

marvelColumns = ["nu1", "nu2", "nu3", "nu4", "L3", "L4", "J", "K", "inv", "Gamma", "Nb", "Em", "Uncertainty", "Transitions"]

marvelEnergies = pd.read_csv("14NH3-NewEnergies.txt", delim_whitespace=True, names=marvelColumns, dtype=str)

statesFileColumns = ["i", "E", "g", "J", "weight", "p", "Gamma", "Nb", "n1", "n2", "n3", "n4", "l3", "l4", "inversion", "J'", "K'", "pRot", "v1", "v2", "v3", "v4", "v5", "v6", "GammaVib", "Calc"]
states = pd.read_csv("../14N-1H3__CoYuTe.states", delim_whitespace=True, names=statesFileColumns, dtype=str)

def generateTagColumn(dataFrame):
    dataFrame["Tag"] = dataFrame["J"] + "-" + dataFrame["Gamma"] + "-" + dataFrame["Nb"]
    return dataFrame

marvelEnergies = generateTagColumn(marvelEnergies)
states = generateTagColumn(states)

states = pd.merge(states, marvelEnergies[["Tag", "Em", "Uncertainty"]], how="left")

def marvelise(row):
    # Parameters that get used for the calculated uncertainty
    deltaB = 0.002
    deltaOmega = 0.3
    if isnan(row["Em"]):
        row["E"] = row["Calc"]
        row["Marvel"] = "Ca"
        row["weight"] = deltaB*int(row["J"])*(int(row["J"]) + 1) + deltaOmega*(int(row["n1"]) + int(row["n2"]) + int(row["n3"]) + int(row["n4"]))
    else:
        row["E"] = row["Em"]
        row["Marvel"] = "Ma"
        row["weight"] = row["Uncertainty"]
    return row

states = states.parallel_apply(lambda x:marvelise(x), result_type="expand", axis=1)
statesFileColumns = ["i", "E", "g", "J", "weight", "p", "Gamma", "Nb", "n1", "n2", "n3", "n4", "l3", "l4", "inversion", "J'", "K'", "pRot", "v1", "v2", "v3", "v4", "v5", "v6", "GammaVib", "Marvel", "Calc"]
states = states[statesFileColumns]
print(states.head(20).to_string(index=False))

states = states.to_string(index=False, header=False)
statesFile = "14N-1H3__CoYuTe-Marvelised-2024.states"
with open(statesFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(states)