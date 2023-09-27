import csv

member_set = set()
with open('ActMembers091323.csv', newline='', encoding='utf-8') as fd:
    csv_reader = csv.reader(fd)
    for row in csv_reader:
        col = row[1]
        member_set.add(row[1])
    for m in member_set:
        print(m)
    print(20 in member_set)