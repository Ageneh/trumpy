import json
import pickle

from trumpytrump import _file_assets, _dir_export, _cached_data_fn, _file_csv_total, _file_csv
from trumpytrump.readDict import readDict
from trumpytrump.wordCount import wordCount



LIWC_de = _file_assets.format("LIWC_de.dic")

cached_data = None
_cache = True


total_wc = 0
data = {}
total_data = {}
total_outlist = {}


def get_cached(fname):
	import os

	if _cache and os.path.exists(fname):
		with open(fname, mode="rb") as stream:
			cached_data, total_wc, total_outlist = pickle.load(stream)
			return cached_data, total_wc, total_outlist
	else:
		cached_data = None
		return None, 0, {}


def export(content, filename):
	import os
	import datetime

	if not os.path.exists("export/output/") or not os.path.isdir("export/output/"):
		os.makedirs("export/output/")

	filename = filename.split(".")

	with open("{}_{}.{}".format((filename[0],
								 str(datetime.datetime.now()),
								 filename[-1])), "w+") as outfile:
		for data in content:
			outfile.write("{},{}".format((data[0], data[1])))
			outfile.write("\n")

	return filename


def get_filenames():
	import os
	global filenames
	dir = _dir_export
	return map(lambda x: "".join((dir, x)), filter(lambda x: x.startswith("export") and x.endswith(".json"), os.listdir(dir)))
	# return ["export/export_harnisch_valid_vor.json"]


def count_rel(total_wc, outlist=total_outlist):
	string = []
	for k, v in list(sorted(outlist.items(), key=lambda x: x[0])):
		string.append("{},{}%".format(k, str(100 * (v / total_wc))))
		# string.append("".join(("{}".format(k), "{}\t".format(v), "{:.5} %\t".format(100 * (v / total_wc)))))
		# string.append("".join(("{:<20}".format(k), "{:<7d}\t".format(v), "{:.5} %\t".format(100 * (v / total_wc)))))
	string.append("{},{}".format("wordcount", total_wc))
	return "\n".join(string)


def cache_fname(fname):
	dir = ""
	if _dir_export in fname:
		spl = fname.split("/")
		dir = "/".join(spl[:-1])
		fname = spl[-1]

	if "." in fname:
		fname = fname.split(".")[0]

	return "{}{}_{}".format(_dir_export, fname, _cached_data_fn)


def export_total_output(data):
	num = 1

	category_names = []

	category_names = [c for c in sorted(data[list(data.keys())[0]]["outList"].keys())]


	with open(_file_csv_total, mode="w", encoding="utf-8") as file:
		file.write(",".join(["#"] + category_names))
		file.write("\n")


		for title, content in data.items():

			line = [num, title, content["publishDate"], content["wc"]]

			categories = [content["outList"][c] for c in sorted(category_names)]

			line += list(categories)

			file.write(",".join(map(lambda x: str(x), line)))
			file.write("\n")

			num += 1


	return


def count():
	finalDict, catList = readDict(LIWC_de)

	for fname in sorted(get_filenames(), reverse=True):
		with open(fname, "r") as file:
			article = json.load(file)

			if fname.split("/")[-1].endswith("_gefiltert.json"):
				continue

			is_total_export = fname.endswith("deutsch.json") # wenn True, dann wird total_output.csv geschrieben

			try:
				articles = [x for x in article]
			except TypeError:
				continue

			if articles == {}:
				continue

			if is_total_export:
				print("Counting words of \'{}\'".format(fname))

			data, total_wc, total_outlist = get_cached(cache_fname(fname))
			if not data:
				data = {}
				for article in articles:

					if not is_total_export:
						try:
							print("Counting words of \'{}\' in \'{}\'".format(article["title"], fname))
						except TypeError as a:
							pass

					res = wordCount(article["content"], finalDict, catList)
					outList, tokens, wc, classified, percClassified = res
					total_wc += classified

					data[article["title"]] = {
						"outList": outList,
						# "tokens": tokens,
						"publishDate": article["publishDate"],
						"wc": wc,
						"classified": classified,
						"percClassified": percClassified
					}

				for d in data.values():
					for k, v in d["outList"].items():
						total_outlist[k] = total_outlist.get(k, 0) + v

				if _cache:
					with open(cache_fname(fname), "wb") as stream:
						pickle.dump((data, total_wc, total_outlist), stream)

			rel = count_rel(total_wc, outlist=total_outlist)
			print(rel)
			print(total_outlist)


			if is_total_export:
				export_total_output(data)

			with open(_file_csv.format(fname.split("/")[-1].split(".")[0]), "w") as output:
				output.write(rel)
				output.write("\n")


if __name__ == '__main__':
	count()