# psyLex: an open-source implementation of the Linguistic Inquiry Word Count
# Created by Sean C. Rife, Ph.D.
# srife1@murraystate.edu // seanrife.com // @seanrife
# Licensed under the MIT License

# Function to count and categorize words based on an LIWC dictionary
import collections
import nltk

from trumpytrump import _file_assets
from nltk.tokenize import RegexpTokenizer
from nltk.stem.cistem import Cistem
from nltk.corpus import stopwords as s_words


stopword_instance = None


def dump_stopword(fname, stopwords):
	'''
	Speichert die cache der stopwords wenn diese noch nicht existieren sollte.
	'''

	import os
	import pickle
	if not os.path.exists(fname):
		with open(fname, "wb") as stream:
			pickle.dump((stopwords), stream)


def load_stopwords(count=0):
	'''
	Lädt die stopwords.
	Sollte die cache für die Stopwords im assets-Ordner vorhanden sein wird diese eingelesen.
	Ansonsten: sollte das Modul nltk vorhanden sein werden die stopwords geladen.
	Falls beides nicht möglich sein wird eine Aufforderung zum Ausführen der benötigten Befehle ausgegeben.

	:param count:
	:return:
	'''

	import os, pickle

	global stopword_instance

	fname = _file_assets.format("stopwords_cache.pkl")

	if count > 1:
		# sollte Problem weiterhin bestehen fordere Nutzer auf
		raise RuntimeError("\n\nYou need to download stopwords.\nPlease run following command in your terminal: \n\nimport nltk\nnltk.download('stopwords')")

	if stopword_instance:
		return stopword_instance

	if os.path.exists(fname):
		with open(fname, mode="rb") as stream:
			stopword_instance = pickle.load(stream)
			return stopword_instance
	else:
		try:
			# versuche die stopwords über nltk herunterzuladen
			stopwords = s_words.words("german")
			# bei Erfolg: speichere die stopwords als pickle-cache
			dump_stopword(fname, stopwords)
			return stopwords
		except BaseException:
			# sollte ein Problem weiterhin bestehen versuche das ganz noch ein weiteres Mal
			nltk.download('stopwords')
			load_stopwords(count=count+1)


def wordCount(data, dictOutput, catList):
	# lade die stopwords
	stopwords = load_stopwords()

	# Create a new dictionary for the output
	outList = collections.OrderedDict()

	# Number of non-dictionary words
	nonDict = 0

	# Convert to lowercase
	data = data.lower().replace("\n", " ")

	# Tokenize and create a frequency distribution
	tokenizer = RegexpTokenizer(r'\w+')
	tokens = tokenizer.tokenize(data)

	fdist = nltk.FreqDist(tokens)
	wc = len(tokens)

	# Using the Cistem stemmer for wildcards, create a stemmed version of the data
	# Cistem: needed for german words/stemming
	cistem = Cistem()

	# wenn ein Wort/Token in den stopwords vorkommt, ignoriere dieses
	# ansonsten: speichere das gestemmte Wort in der Liste
	stems = [cistem.stem(word) for word in tokens if word not in stopwords and len(word) > 0]
	fdist_stem = nltk.FreqDist(stems)

	# Access categories and populate the output dictionary with keys
	for cat in catList:
		outList[cat[0]] = 0

	# Dictionaries are more useful
	fdist_dict = dict(fdist)
	fdist_stem_dict = dict(fdist_stem)

	# Number of classified words
	classified = 0

	for key in dictOutput:
		if "*" in key and key[:-1] in fdist_stem_dict:
			classified = classified + fdist_stem_dict[key[:-1]]
			for cat in dictOutput[key]:
				if cat.isalpha():
					outList[cat] = outList[cat] + fdist_stem_dict[key[:-1]]
		elif key in fdist_dict:
			classified = classified + fdist_dict[key]
			for cat in dictOutput[key]:
				try:
					outList[cat] = outList[cat] + fdist_dict[key]
				except KeyError:
					pass

	# Calculate the percentage of words classified
	if wc > 0:
		percClassified = (float(classified) / float(wc)) * 100
	else:
		percClassified = 0

	# Return the categories, the words used, the word count, the number of words classified,
	# and the percentage of words classified.
	return [outList, tokens, wc, classified, percClassified]
