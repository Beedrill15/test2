#This program is to generate random responses for customer service surveys
from random import randint

#First create the file where the results will be stored
record = open("survey_responses/record_results.csv","w")
#Then generate j customers' responses
for j in range(0,10):
	#The first number determines the product
	response = [randint(1,5)]
	#The next 10 numbers determine how well a product did in various categories based on a scale from one to ten
	for i in range(0,10):
		response.append(randint(1,10))
	#Then there might be an optional response to the last question
	if response[1] < 4:
		response.append("The product was unsatisfactory")
	elif response[1] > 7:
		response.append("The product was exceptional")
	#Finally the respose is recorded
	record.write(str(response)+"\n")
