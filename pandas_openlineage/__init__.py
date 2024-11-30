import pandas as pd
from pandas_openlineage.decorators.lineage import lineage_read, lineage_write


def monkey_patch_pandas():
    setattr(pd, "read_csv", lineage_read("filepath_or_buffer")(pd.read_csv))
    setattr(pd.DataFrame, "to_csv", lineage_write("path_or_buf")(pd.DataFrame.to_csv))


# Apply the monkey patch when the module is imported
monkey_patch_pandas()

before_import = set(locals())

# Replace the pandas module in sys.modules with the monkey-patched version
from pandas import *  # noqa: E402,F403

__all__ = set(locals()) - set(before_import)
