import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

columns = ["nu", "unc1", "unc2", "nu1'", "nu2'", "nu3'", "nu4'", "L3'", "L4'", "J'", "K'", "inv'", "Gamma'", "Nb'",
                      "nu1\"", "nu2\"", "nu3\"", "nu4\"", "L3\"", "L4\"", "J\"", "K\"", "inv\"", "Gamma\"", "Nb\"", "Source", "E'", "Calc'", "Diff", "int", "Int", 
                      "ExpRatio", "CalcRatio", "DiffRatio"]

transitions = pd.read_csv("18ZoCoOvKyAgainstStatesFile.txt", delim_whitespace=True, names=columns)

def generateBandTags(row):
    row["BandTag"] = str(row["nu1'"]) + "-" + str(row["nu2'"]) + "-" + str(row["nu3'"]) + "-" + str(row["nu4'"]) \ 
        + "-" str(row["L3'"]) + "-" + str(row["L4'"])
    return row

transitions = transitions.parallel_apply(lambda x:generateBandTags(x), result_type="expand", axis=1)

