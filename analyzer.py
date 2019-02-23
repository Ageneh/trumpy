import json
from datetime import datetime, timedelta
from dateutil import parser
import re
import pytz

from multiprocessing.pool import ThreadPool
from trumpytrump import _file_assets, _file_export, _dir_export
import word_counter


utc = pytz.UTC

# dateinamen
_base_filename = _file_assets.format("{}_{{}}.json".format("export_harnisch"))
_base_filename_exp = _file_export.format("{}_{{}}.json".format("export_harnisch"))
_filename = _base_filename.format("valid")
_fn_german = _base_filename_exp.format("valid_deutsch")
_fn_german_pre = _base_filename_exp.format("valid_vor")
_fn_german_post = _base_filename_exp.format("valid_nach")
_fn_german_post_filtered = _base_filename_exp.format("valid_nach_gefiltert")


# zeitspanne
scandal_date = datetime(year=2017, month=4, day=2).replace(tzinfo=utc)
delta = timedelta(weeks=4)
pre_date = scandal_date - delta
post_date = scandal_date + delta


# resultat speicher
result = {
	"pre": [],
	"post": [],
	"all": [],
	"allSpan": [],
}


def export(content, filename):
	try:
		import os

		dir = _dir_export
		if not os.path.exists(dir) or not os.path.isdir(dir):
			os.makedirs(dir)

		# path ~ path to filtered json
		with open(filename, "w+") as outfile:
			json.dump(content, outfile, indent=4, sort_keys=True)
	except TypeError:
		for entry in content:
			entry["publishDate"] = str(entry["publishDate"])
		export(content, filename)


def reset_dir():
	import shutil
	shutil.rmtree(_dir_export, ignore_errors=True)


def start():
	# path ~ path to json
	print(_filename)

	import os
	print(os.listdir("."))

	cwd = os.getcwd()  # Get the current working directory (cwd)
	files = os.listdir(cwd)  # Get all the files in that directory
	print("Files in '%s': %s" % (cwd, files))

	with open(_filename, mode="r", encoding="utf-8") as file:
		file_json = json.load(file)
	print("Anzahl aller Artikel:", len(file_json))

	# herausfiltern aller deutschen artikel
	for x in file_json:
		if re.search("(country_de_Deutschland)", x.get("tags2", "")):
			publishDate = parser.parse(x["publishDate"]).replace(tzinfo=utc)
			x["publishDate"] = publishDate

			result["all"].append(x["id"])

			if pre_date <= publishDate <= post_date:
				result["allSpan"].append(x["id"])

			if pre_date <= publishDate < scandal_date:
				# vor skandal
				result["pre"].append(x["id"])
			elif scandal_date <= publishDate <= post_date:
				# nach skandal
				result["post"].append(x["id"])

	content_dict = {x["id"]: x for x in file_json}  # in ein dict umwandeln
	file_json = None

	reset_dir()

	exp = [content_dict[k] for k in result["all"]]
	print("Anzahl der deutschen Artikel:", len(exp))
	export(exp, _fn_german)

	exp = [content_dict[k] for k in result["pre"]]
	print("Anzahl der deutschen Artikel ({} - {}):".format(str(post_date), str(scandal_date)), len(exp))
	export(exp, _fn_german_pre)

	exp = [content_dict[k] for k in result["post"]]
	print("Anzahl der deutschen Artikel ({} - {}):".format(str(scandal_date), str(post_date)), len(exp))
	export(exp, _fn_german_post)

	exp = None

	# filtern nach keywords
	keywords = {"rakete"}
	filtered_data = {}
	for id in result["post"]:
		article = content_dict[id]
		stem_words = re.findall("\w+", article["content"].lower())

		p_date = parser.parse(article["publishDate"]).replace(tzinfo=utc)
		if p_date.year not in filtered_data.keys():
			filtered_data[p_date.year] = {}
		year_data = filtered_data.get(p_date.year, {})

		for word in set(stem_words):
			word_data = year_data.get(word, [])  # kategorisiert nach wort
			if word in keywords:
				word_data.append(article)
				filtered_data[p_date.year][word] = word_data

	export(filtered_data, _fn_german_post_filtered)
	print("Anzahl der gefilterten Artikel ({} - {}):".format(str(scandal_date), str(post_date)))
	for year in sorted(filtered_data.keys()):
		print("Jahr:", year)
		for w in sorted(filtered_data[year]):
			articles_per_word = filtered_data[year][w]
			print("\"{}\":".format(w), ", ".join(["\'{}\'".format(x["title"]) for x in articles_per_word]))
		print("")

	# print(filtered_data)

	return


if __name__ == '__main__':
	start()

	if input("WordCount errechnen? (y/n) - ").lower() == "y":
		word_counter.count()
