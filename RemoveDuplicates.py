weekdays = ['sun','mon','tue','sun','mon','tue','wed','thu','fri','wed','thu','fri','sat']

dupRemoved = []
for day in weekdays:
	if day not in dupRemoved:
		dupRemoved.append(day)

print(dupRemoved)
