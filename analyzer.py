from trumpytrump import *
from trumpytrump import _file_assets, _file_export, _dir_export
from weekly_counter import WeeklyCounter
from word_counter import WordCounter
from trumpytrump import fn_german, fn_german_post, fn_german_post_filtered, fn_german_pre
from trumpytrump import _filename, base_filename, base_filename_exp


class Analyzer:

	def __init__(self, keywords):
		# zeitspanne
		y, m, d, weeks = parse_argv()

		# erstellen der Zeitspanne relativ zum Datum des Skandals
		self.scandal_date = datetime(year=y, month=m, day=d).replace(tzinfo=utc)
		self.delta = timedelta(weeks=weeks)
		self.pre_date = scandal_date - self.delta
		self.post_date = scandal_date + self.delta

		print("Von {} bis {}".format(self.pre_date,self.post_date))

		# sollten keywords mitgegben sein sollen diese als menge gespeichert werden
		self.keywords = set(map(lambda x: x.lower(), keywords)) if keywords else set()
		self.result = {
			"pre": [],  # eine Liste mit den Titeln aller Artikel vor dem Skandal
			"post": [],  # eine Liste mit den Titeln aller Artikel nach dem Skandal
			"all": [],  # eine Liste mit allen Titeln
			"allSpan": [],  # eine Liste mit allen Titeln innerhalb der Zeitspanne
			"postFiltered": {},  # ein dictionairy wo die gefilterten artikel gespeichert werden
		}
		self.content_dict = {}

		# ein Lambda-Ausdruck um einen String in ein dateteime-Objekt umzuwandeln
		self.to_datetime = lambda x: parser.parse(x).replace(tzinfo=utc)

		# lese den Inhalt der JSON-Datei
		with open(_filename, mode="r", encoding="utf-8") as file:
			self.file_json = json.load(file)  # der Ursprungs-Inhalt
			self.jsonDict = {x["title"]: x for x in self.file_json}  # der Ursprungs-Inhalt als Dictionary

			# formatiere das Datum in ein datetime-Objekt für jeden Artikel
			for k in self.jsonDict:
				date = self.jsonDict[k]["publishDate"]
				self.jsonDict[k]["publishDate"] = self.to_datetime(date)

		return

	def filter(self):
		'''
		Filtert die Ursprungs-JSON und speichert die Resultate in self.result.
		'''

		for title, entry in self.jsonDict.items():
			# iteriere durch alle Artikel

			# herausfiltern aller deutschen artikel
			if re.search("(country_de_Deutschland)", entry.get("tags2", "")):
				# wenn "country_de_Deutschland" in dem Wert für "tags2" vorliegt

				publishDate = entry["publishDate"]  # speichere das Datum

				# sollte der aktuelle Artikel noch nicht in self.result["all"] vorhanden sein speichere den Titel
				# ansonsten gehe weiter zum nächsten Artikel
				if title not in self.result["all"]:
					self.result["all"].append(entry["title"])
				else:
					continue

				if self.pre_date <= publishDate <= self.post_date and title not in self.result["allSpan"]:
					self.result["allSpan"].append(entry["title"])

				if self.pre_date <= publishDate < scandal_date and title not in self.result["pre"]:
					self.result["pre"].append(entry["title"])  # vor skandal

				elif scandal_date <= publishDate <= self.post_date and title not in self.result["post"]:
					self.result["post"].append(entry["title"])  # nach skandal


		self.content_dict = {x["title"]: x for x in self.file_json if x["title"] in self.result["all"]}  # in ein dict umwandeln

		filtered_data = {}
		# filtern nach keywords
		for title in self.result["post"]:
			article = self.content_dict[title]
			# speichere alle Wörter aus dem Artikel in einer Liste
			stem_words = re.findall("\w+", article["content"].lower())

			p_date = article["publishDate"]

			# erstelle einen Eintrag für das Jahr des Artikels in filtered_data
			if p_date.year not in filtered_data.keys():
				filtered_data[p_date.year] = {}
			year_data = filtered_data.get(p_date.year, {})

			# für jedes Wort in stem_words wird geschaut ob das wort in den keywords vorkommt
			for word in set(stem_words):
				word_data = year_data.get(word, [])  # kategorisiert nach wort
				if word in self.keywords:
					# wenn das Wort in den keywords vorkommt, speichere den Titel in filtered_data
					word_data.append(article)
					filtered_data[p_date.year][word] = word_data

			# wenn es keinen Eintrag in dem aktuellen Jahr gibt, wird dieses Jahr gelöscht
			if len(filtered_data[p_date.year]) < 1:
				filtered_data.pop(p_date.year)

		self.result["postFiltered"] = filtered_data
		return

	def export(self):
		reset_dir()

		exp = [self.content_dict[k] for k in self.result["all"]]
		print("Anzahl der deutschen Artikel:", len(exp))
		export(exp, fn_german)

		exp = [self.content_dict[k] for k in self.result["pre"]]
		print("Anzahl der deutschen Artikel ({} - {}):".format(str(self.pre_date), str(scandal_date)), len(exp))
		export(exp, fn_german_pre)

		exp = [self.content_dict[k] for k in self.result["post"]]
		print("Anzahl der deutschen Artikel ({} - {}):".format(str(scandal_date), str(self.post_date)), len(exp))
		export(exp, fn_german_post)

		export(self.result["postFiltered"], fn_german_post_filtered)
		if len(self.result["postFiltered"]) > 0:
			print("Gefilterte Artikel ({} - {}):".format(str(scandal_date), str(self.post_date)))
			filtered = self.result["postFiltered"]
			for year in sorted(filtered.keys()):
				print("Jahr:", year)
				for w in sorted(filtered[year]):
					articles_per_word = filtered[year][w]
					print("\"{}\": {:<5} Artikel".format(w, str(len(articles_per_word))))
				print("")

		return

	def start(self):
		self.filter()
		self.export()  # exportiere alle Ergebnisse

		print("- " * 20)
		print("Mit Duplikate:")
		print("Alle Artikel:", len(self.file_json))
		print("Deutsche Artikel:", len(set(x["id"] for x in self.file_json if re.search("(country_de_Deutschland)", x.get("tags2", "")))))
		print("- " * 20)
		print("Ohne Duplikate:")
		print("Alle Artikel: {}".format(len(set(x["title"] for x in self.file_json))))
		print("Deutsche Artikel: {}".format(len(self.result["all"])))
		print("Deutsche Artikel in Zeitspanne: {}".format(len(self.result["allSpan"])))
		print("Deutsche Artikel vor Skandal: {}".format(len(self.result["pre"])))
		print("Deutsche Artikel nach Skandal: {}".format(len(self.result["post"])))
		print("- " * 20)
		return


def export(content, filename):
	'''
	Exportiert den gegebenen Inhalt unter dem gegebenen Dateinamen.
	Bei einen TypeError wird export() erneut aufgerufen. Davor wird jedoch das Datum in einen String umgewandelt.

	:param content: Der Inhalt welcher exportiert werden soll.
	:param filename: Der Datei-Pfad unter welchem der Inhalt gespeichert werden soll.
	'''

	try:
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
	'''
	Löscht den export-Ordner und den Inhalt.
	'''

	import shutil
	shutil.rmtree(_dir_export, ignore_errors=True)
	return


def parse_argv(year=2017, month=4, day=2, duration=4):
	'''
	Nimmt aus den Start-Argumenten die gegebenen Daten.
	Sollte "-d" vorhanden sein, wird nach dem Datum gesucht. Das Datum muss im format YYYY-MM-DD vorliegen.
	Sollte "-w" vorhanden sein, wird nach der Wochen-Anzahl für den Zeitraum um den Skandal herum gesucht.

	Beispiel:
	analyzer.py -d 2016-04-23 -w 6

	Bei einer Fehleingabe wie einem falschen Format wird das Programm beendet.

	:param year: Das Jahr des Skandals. Hat einen Standard Wert, kann aber auch einfach überschrieben werden.
	:param month: Der Monat des Skandals. Hat einen Standard Wert, kann aber auch einfach überschrieben werden.
	:param day: Der Taf des Skandals. Hat einen Standard Wert, kann aber auch einfach überschrieben werden.
	:param duration: Die Anzahl der Woche vor und nach dem Skandal, in welchen gesucht werden soll. Hat einen Standard Wert, kann aber auch einfach überschrieben werden.
	:return: Gibt ein tuple-Objekt im Format (Jahr, Monat, Tag, Wochen) zurück.
	'''

	global scandal_date
	scandal_date = datetime(year=year, month=month, day=day).replace(tzinfo=utc)

	def exit():
		raise OSError("Given arguments are invalid: {}".format(" ".join(argv[1:])))

	_date = "-d"
	_span = "-w"
	if _date in argv:
		idx = argv.index(_date)
		date_arg = argv[idx + 1]
		date_arg = date_arg.split("-")
		if len(date_arg) < 3:
			exit()
		else:
			try:
				year, month, day = list(map(lambda x: int(x), date_arg))
			except:
				exit()

			scandal_date = datetime(year=year, month=month, day=day).replace(tzinfo=utc)

	if _span in argv:
		idx = argv.index(_span)
		dur_arg = argv[idx + 1]
		try:
			duration = int(dur_arg)
		except ValueError:
			exit()

	return year, month, day, duration


if __name__ == '__main__':
	started = datetime.now()

	keywords = ["Familie"]  # erstelle eine liste mit keywords
	analyzer = Analyzer(keywords).start()  # erstelle eine Analyzer-Instanz und starte diese

	ended = datetime.now()
	elapsed = ended - started
	print("Analyzer time: {}".format(elapsed))

	if input("WordCount errechnen? (y/n) - ").lower() == "y":
		# wenn vom User gewollt, wird die Wortanzahl berechnet
		ended = None

		# erstlle WordCounter-Instanz und starte diese
		counter = WordCounter()
		counter.start()

		# erstlle WeeklyCounter-Instanz
		weekly = WeeklyCounter(counter)
		weekly.start()

	if not ended: ended = datetime.now()

	elapsed = ended - started
	print("Time started: {}".format(started))
	print("Time ended: {}".format(ended))
	print("Total time running: {}".format(elapsed))
