_dir_root = "trumpytrump"
_dir_assets = "{}/".format("assets")
_dir_export = "{}/".format("export")

_file_root = "{}/{{}}".format(_dir_root)
_file_assets = "{}{{}}".format(_dir_assets)
_file_export = "{}{{}}".format(_dir_export)

_file_csv = _file_export.format("{}_output.csv")
_file_csv_total = _file_export.format("total_output.csv")

_cached_data_fn = "cache_data.pkl"