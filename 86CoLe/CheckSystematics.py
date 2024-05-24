import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

transitionsColumns = ["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "inv'", "Gamma'", "Nb'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "inv\"", "Gamma\"", "Nb\"", "Source"]
transitions = pd.read_csv("86CoLeMarvel.txt", delim_whitespace=True, names=transitionsColumns)
transitionsNew =  pd.read_csv("../18ZoCoOvKy/18ZoCoOvKyMarvel.txt", delim_whitespace=True, names=transitionsColumns)
transitions["Paper"] = "86CoLe"
transitionsNew["Paper"] = "18ZoCoOvKy"
transitionsNew["nu-new"] = transitionsNew["nu"]
def generateTag(row):
    row["Tag"] = (str(row["J'"]) + "-" + str(row["Gamma'"]) + "-" + str(row["Nb'"])
                  + str(row["J\""]) + "-" + str(row["Gamma\""]) + "-" + str(row["Nb\""]))
    return row
transitions = transitions.parallel_apply(lambda x: generateTag(x), result_type="expand", axis=1)
transitionsNew = transitionsNew.parallel_apply(lambda x: generateTag(x), result_type="expand", axis=1)
transitions = transitions.merge(transitionsNew[["nu-new", "Tag"]], on="Tag")
transitions["Systematic"] = transitions["nu-new"] - transitions["nu"]
transitions = transitions.to_string(index=False, header=False)
marvelFile = "86CoLeSystematics.txt"
with open(marvelFile, "w+") as FileToWriteTo:
    FileToWriteTo.write(transitions)