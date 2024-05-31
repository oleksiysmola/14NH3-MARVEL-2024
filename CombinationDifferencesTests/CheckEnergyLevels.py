import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

marvelColumns = ["nu1", "nu2", "nu3", "nu4", "L3", "L4", "J", "K", "inv", "Gamma", "Nb", "E", "Uncertainty", "Transitions"]

marvelEnergies = pd.read_csv("14NH3-MarvelEnergies-2024.txt", delim_whitespace=True, names=marvelColumns)
# marvelEnergies = pd.read_csv("14NH3-MarvelEnergies-2020.txt", delim_whitespace=True, names=marvelColumns)

marvelEnergies = marvelEnergies.groupby(["J", "Gamma"])

def checkForCloseLevels(dataFrame):
    dataFrame = dataFrame.sort_values("E")
    problem = []
    if len(dataFrame) > 1:
        if dataFrame.iloc[1]["E"] - dataFrame.iloc[0]["E"] < 0.05:
            problem += [True]
        else: 
            problem += [False]
        for i in range(1, len(dataFrame) - 1):
            if dataFrame.iloc[i]["E"] - dataFrame.iloc[i - 1]["E"] < 0.05:
                problem += [True]
            elif dataFrame.iloc[i + 1]["E"] - dataFrame.iloc[i]["E"] < 0.05:
                problem += [True]
            else: 
                 problem += [False]
        if dataFrame.iloc[len(dataFrame) - 1]["E"] - dataFrame.iloc[len(dataFrame) - 2]["E"] < 0.05:
            problem += [True]
        else: 
            problem += [False]
    else:
        problem += [False]
    dataFrame["Problem"] = problem
    return dataFrame
marvelEnergies = marvelEnergies.parallel_apply(lambda x:checkForCloseLevels(x))
print(marvelEnergies[marvelEnergies["Problem"]].to_string(index=False))
print(len(marvelEnergies[marvelEnergies["Problem"]]))