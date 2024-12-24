HAVE = "already-have.txt"
FULL = "SraAccList.csv"

with open(HAVE) as f1:
    have_lines = set(f1)

with open(FULL) as f2:
    full_lines = set(f2)

# matches = [line for line in full_lines if line in have_lines]
# matches.sort()
# print(matches)
# f3 = open("matches.txt", "w")
# f3.write("".join(matches))

missing = [line for line in full_lines if line not in have_lines]
f5 = open("missing.txt", "w")
f5.write("".join(missing))

missingSRR = [line[3:] for line in missing if line[0:3]=="SRR"]
missingSRR.sort(key=int)

f4 = open("missingSRR.txt", "w")
f4.write("".join(missingSRR))
