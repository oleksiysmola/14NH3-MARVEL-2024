import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

marvelColumns = ["nu1", "nu2", "nu3", "nu4", "L3", "L4", "J", "K", "inv", "Gamma", "Nb", "E", "Uncertainty", "Transitions"]

marvelEnergies2020 = pd.read_csv("14NH3-MarvelEnergies-2020.txt", delim_whitespace=True, names=marvelColumns)
marvelEnergiesNew = pd.read_csv("14NH3-NewEnergies.txt", delim_whitespace=True, names=marvelColumns)

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

marvelEnergies2020 = marvelEnergies2020.parallel_apply(lambda x:checkIf2020LevelIsPresent(x, marvelEnergiesNew), result_type="expand", axis=1)

missing2020Levels = marvelEnergies2020[marvelEnergies2020["Present"] == False]
print(len(missing2020Levels))
print(missing2020Levels.to_string(index=False))