import sys
import json
import csv

# It goes like this: accepts three command line arguments: (1) mode - see below - (2) text to analyze (file vs. raw text), and (3) the path to the dictionary file we should use.
from trumpytrump import readDict
from trumpytrump import wordCount

inMode = sys.argv[1]
inValue = sys.argv[2]
inDict = sys.argv[3]

dictIn = readDict(inDict)

# Currently supports three modes: basic text (entered as a command-line argument), file-based (will read a whole frakin' file) and json input using 'content' as resource.
# If basic text mode, just assign the input value the value of the second command line argument.
if inMode == "0":
	inData = inValue
# If file mode, read the file into a string.
elif inMode == "1":
	with open(inValue, "r") as myfile:
		inData = myfile.read().replace('\n', ' ').replace(".", " . ").replace("!", " ! ").replace("?", " ? ")
# If json mode with 'content' as resource, read file and extract 'content' string from json
elif inMode == "2":
	with open(inValue, "r") as myfile:
		input_data = myfile.read()
	inData_json = json.loads(input_data, strict=False)
	inData = inData_json["content"].replace('\n', ' ')

# Run the wordCount function using the specified parameters.
out = wordCount(inData, dictIn[0], dictIn[1])

# print(out[0].items())

for k, v in out[0].items():
	print(k + ": " + str(v))

with open('output.csv', 'w') as csvfile:
	writer = csv.writer(csvfile)
	writer.writerows(out[0].items())
	csvfile.write("wordcount," + str(out[2]))
