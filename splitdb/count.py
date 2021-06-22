import os

database = "retail"
interval = 40000

# dbSize = 0
# for filename in os.listdir(f"incdatasets/interval_{database}_{interval}/"):
#     if filename.endswith(".txt"):
#         with open(os.path.join(f"incdatasets/interval_{database}_{interval}", filename), 'r') as f:
#             for line in f:
#                 if line:
#                     dbSize += 1
# print(dbSize)


inc_number = 0 
incDBPath = f"incdatasets/interval_{database}_{interval}/db_{inc_number}.txt"
while os.path.isfile(incDBPath):
    print(incDBPath)
    inc_number += 1
    incDBPath = f"incdatasets/interval_{database}_{interval}/db_{inc_number}.txt"
