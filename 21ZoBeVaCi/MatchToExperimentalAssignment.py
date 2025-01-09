import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

transitionsColumns = ["nu", "int", "J'", "K'", "Gamma'", "<-", "J\"", "K\"", "Gamma\"", "Calc'", "<-'", "Calc\"", "nuCalc", "v1'", "v2'", "v3'", "v4'", "v5'", "v6'", "<-''",
                      "v1\"", "v2\"", "v3\"", "v4\"", "v5\"", "v6\"", "Int"]


transitions = pd.read_csv("21ZoBeVaCi-AssignedTransitions.txt", delim_whitespace=True, names=transitionsColumns)
transitions = transitions[transitions["nu"] > 0]

transitions = transitions.dropna()

quantumNumbers = ["J'", "K'", "J\"", "K\"", "v1'", "v2'", "v3'", "v4'", "v5'", "v6'",
                      "v1\"", "v2\"", "v3\"", "v4\"", "v5\"", "v6\""]

for quantumNumber in quantumNumbers:
    transitions[quantumNumber] = transitions[quantumNumber].astype(int)
print(transitions.head(20).to_string(index=False))
print(len(transitions))

empiricalUpperStates = pd.read_csv("21ZoBeVaCi-EmpiricalEnergies.txt", delim_whitespace=True)[["nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "inv'", "Calc'", "Eobs'"]]
print(empiricalUpperStates.to_string(index=False))
transitions = pd.merge(transitions, empiricalUpperStates, on="Calc'", how="left")
print(transitions.to_string(index=False))
