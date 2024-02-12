import pandas as pd
from pandarallel import pandarallel
pandarallel.initialize(progress_bar=True)

transFileColumns = ["i'", "i\"", "I"]

listOfTransFiles = ["../14N-1H3__CoYuTe__18000-18100.trans", 
    "../14N-1H3__CoYuTe__18100-18200.trans"
    ]
trans = []
for transFile in listOfTransFiles:
    print(f"Reading transition file: {transFile}")
    trans += [pd.read_csv(transFile, names=transFileColumns, delim_whitespace=True)]
trans = pd.concat(trans)
print(trans.head(15).to_string(index=False))
transFiltered1 = trans[trans["i\""] == 63235]
transFiltered1 = transFiltered1[transFiltered1["i'"] >= 14193].head(3)
transFiltered2 = trans[trans["i\""] == 7779]
transFiltered2 = transFiltered2[transFiltered2["i'"] >= 14193].head(3)
print(transFiltered1.to_string(index=False))
print(transFiltered2.to_string(index=False))
