import random

randomList = []
notThere = 0
for i in range(random.randint(1,5)):
	randomList.append(random.randint(-2,5))

#print(len(randomList))
print(randomList)

for y in range(1,(len(randomList)+1)):
	print("We are checking for " + str(y))
	for x in randomList:
		print("Does it equal " + str(x))
		if x==y:
			print("It does. This is not the number we are looking for.")
			notThere=0
			break
		else:
			print("It does not.")
			notThere=1
	if notThere==1:
		print(str(y) + " is the lowest missing positive integer.")
		break
		
if notThere == 0:
	print(str(len(randomList)+1) + " is the lowest missing positive integer")
