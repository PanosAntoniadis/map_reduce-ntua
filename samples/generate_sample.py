import random

# Simple script that generates a pseudo-dataset for testing k-means

with open("sample.csv", 'w') as f:
	for i in range(10):
		date_in = random.randint(0, 9)
		date_out = random.randint(0, 9)
		x_in = random.randint(0, 9)
		y_in = random.randint(0, 9)
		x_out = random.randint(0, 9)
		y_out = random.randint(0, 9)
		cost = random.randint(0, 100)
		f.write(str(i) + "," + str(date_in) + "," + str(date_out) + "," + str(x_in) + "," + str(y_in) + "," + str(x_out) + "," + str(y_out) + "," + str(cost))
		f.write("\n")
