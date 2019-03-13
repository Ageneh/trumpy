from trumpytrump import *
from trumpytrump import _file_export, _dir_export
from trumpytrump import _filename
from trumpytrump import fn_german, fn_german_post, fn_german_post_filtered, fn_german_pre
from weekly_counter import WeeklyCounter
from word_counter import WordCounter


class Analyzer:

    def __init__(self, keywords, overwrite=False):
        self.overwrite = True if overwrite else False

        # zeitspanne
        y, m, d, weeks = parse_argv()
        self.scandal_date = datetime(year=y, month=m, day=d).replace(tzinfo=utc)
        self.delta = timedelta(weeks=weeks)
        self.pre_date = scandal_date - self.delta
        self.post_date = scandal_date + self.delta

        print("Von {} bis {}".format(self.pre_date, self.post_date))

        self.keywords = set(map(lambda x: x.lower(), keywords))
        self.filtered_data = {}
        self.result = {
            "pre": [],
            "post": [],
            "all": [],
            "allSpan": [],
            "postFiltered": {},
        }
        self.content_dict = {}

        with open(_filename, mode="r", encoding="utf-8") as file:
            self.file_json = json.load(file)
            self.jsonDict = {x["title"]: x for x in self.file_json}
            for k in self.jsonDict:
                date = self.jsonDict[k]["publishDate"]
                self.jsonDict[k]["publishDate"] = parser.parse(date).replace(tzinfo=utc)

        print("Anzahl aller Artikel (mit/ohne Duplikate): {} / {}".format(len(self.file_json), len(self.jsonDict)))
        print("Anzahl aller deutschen Artikel (mit Duplikate): {}".format(len([x for x in self.file_json if re.search("(country_de_Deutschland)", x.get("tags2", ""))])))

        return

    def filter(self):

        for title, entry in self.jsonDict.items():
            # herausfiltern aller deutschen artikel
            if re.search("(country_de_Deutschland)", entry.get("tags2", "")):
                publishDate = entry["publishDate"]

                if title not in self.result["all"]:
                    self.result["all"].append(entry["title"])

                if self.pre_date <= publishDate <= self.post_date and title not in self.result["allSpan"]:
                    self.result["allSpan"].append(entry["title"])

                if self.pre_date <= publishDate < scandal_date and title not in self.result["pre"]:
                    self.result["pre"].append(entry["title"])  # vor skandal

                elif scandal_date <= publishDate <= self.post_date and title not in self.result["post"]:
                    self.result["post"].append(entry["title"])  # nach skandal

        print("Anzahl aller deutschen Artikel (ohne Duplikate): {}".format(len(self.result["all"])))
        print("Anzahl aller deutschen Artikel in Zeitspanne (ohne Duplikate): {}".format(len(self.result["allSpan"])))
        print("Anzahl aller deutschen Artikel vor Skandal (ohne Duplikate): {}".format(len(self.result["pre"])))
        print("Anzahl aller deutschen Artikel nach Skandal (ohne Duplikate): {}".format(len(self.result["post"])))

        print("")

        self.content_dict = {x["title"]: x for x in self.file_json if
                             x["title"] in self.result["all"]}  # in ein dict umwandeln

        # filtern nach keywords
        filtered_data = {}
        for article in map(lambda x: self.content_dict[x], self.result["post"]):
            stem_words = re.findall("\w+", article["content"].lower())

            p_date = article["publishDate"]
            if p_date.year not in filtered_data.keys():
                filtered_data[p_date.year] = {}
            year_data = filtered_data.get(p_date.year, {})

            for word in set(stem_words):
                word_data = year_data.get(word, [])  # kategorisiert nach wort
                if word in self.keywords:
                    word_data.append(article)
                    filtered_data[p_date.year][word] = word_data

        self.content_dict["postFiltered"] = filtered_data

        return

    def export(self):
        reset_dir()

        if not file_exists(fn_german):
            exp = [self.content_dict[k] for k in self.result["all"]]
            print("Anzahl der deutschen Artikel:", len(exp))
            export(exp, fn_german)

        exp = [self.content_dict[k] for k in self.result["pre"]]
        print("Anzahl der deutschen Artikel ({} - {}):".format(str(self.pre_date), str(scandal_date)), len(exp))
        export(exp, fn_german_pre)

        exp = [self.content_dict[k] for k in self.result["post"]]
        print("Anzahl der deutschen Artikel ({} - {}):".format(str(scandal_date), str(self.post_date)), len(exp))
        export(exp, fn_german_post)

        export(self.content_dict["postFiltered"], fn_german_post_filtered)
        if len(self.content_dict["postFiltered"]) > 0:
            print("Gefilterte Artikel ({} - {}):".format(str(scandal_date), str(self.post_date)))
            for year in sorted(self.filtered_data.keys()):
                print("Jahr:", year)
                for w in sorted(self.filtered_data[year]):
                    articles_per_word = self.filtered_data[year][w]
                    print("\"{}\":".format(w), ", ".join(["\'{}\'".format(x["title"]) for x in articles_per_word]))
                print("")

        return

    def start(self):
        self.filter()
        self.export()


def export(content, filename):
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
    import shutil

    if os.path.exists(fn_german):
        for file in os.listdir(_dir_export):
            path = _file_export.format(file)
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
            elif fn_german != path:
                os.remove(path)

    else:
        shutil.rmtree(_dir_export, ignore_errors=True)

    return


def parse_argv(year=2017, month=4, day=2, duration=4):
    global scandal_date
    scandal_date = datetime(year=year, month=month, day=day).replace(tzinfo=utc)

    def exit():
        raise OSError("Given arguments are invalid: {}".format(" ".join(argv[1:])))

    _date = "-d"
    _duration = "-w"
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

    if _duration in argv:
        idx = argv.index(_duration)
        dur_arg = argv[idx + 1]
        try:
            duration = int(dur_arg)
        except ValueError:
            exit()

    return year, month, day, duration


if __name__ == '__main__':
    started = datetime.now()

    keywords = ["die"]
    analyzer = Analyzer(keywords).start()

    print("Analyzer time: {}".format(str(datetime.now() - started)))

    if input("WordCount errechnen? (y/n) - ").lower() == "y":
        counter = WordCounter()
        counter.start()

        weekly = WeeklyCounter(counter)
        weekly.start()

    ended = datetime.now()
    print("Time started: {}".format(ended - started))
    print("Time ended: {}".format(ended - started))
    print("Total time running: {}".format(ended - started))
