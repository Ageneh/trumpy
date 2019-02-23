_dir_root = "trumpytrump"
_dir_assets = "{}/".format("assets")
_dir_export = "{}/".format("export")
_dir_csv = "{}{}/".format(_dir_export, "csv")

_file_root = "{}/{{}}".format(_dir_root)
_file_assets = "{}{{}}".format(_dir_assets)
_file_export = "{}{{}}".format(_dir_export)

_file_csv = _file_export.format("csv/{}_output.csv")
_file_csv_total = _file_export.format("csv/total_output.csv")

_cached_data_fn = "cache_data.pkl"


# dateinamen
base_filename = _file_assets.format("{}_{{}}.json".format("export_harnisch"))
base_filename_exp = _file_export.format("{}_{{}}.json".format("export_harnisch"))
filename = base_filename.format("valid")
fn_german = base_filename_exp.format("valid_deutsch")
fn_german_pre = base_filename_exp.format("valid_vor")
fn_german_post = base_filename_exp.format("valid_nach")
fn_german_post_filtered = base_filename_exp.format("valid_nach_gefiltert")


suffix_filtered = "_gefiltert"