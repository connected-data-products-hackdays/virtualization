"""
Iterate OPR data site and generate manifest files for each dataproduct.
"""
import xopr
import virtualizarr
import requests
import h5py
import xarray as xr


from obspec_utils.registry import ObjectStoreRegistry
from obspec_utils.stores import AiohttpStore

from pathlib import Path
import os


BASE_URL = "https://data.cresis.ku.edu"

TEST_URL = "https://data.cresis.ku.edu/data/rds/2016_Greenland_Polar6/CSARP_standard/20160517_06/Data_20160517_06_001.mat"

MATLAB_INTERNAL_GROUPS = [
    "#refs#",
    "#subsystem#",
    "param_array",
    "param_records",
    "param_sar",
    "file_type",
    "file_version",
    "radiometric_corr_dB",
]

# store = AiohttpStore(BASE_URL)
# registry = ObjectStoreRegistry({BASE_URL: store})

from obstore.store import LocalStore

MANIFESTS_DIR = "manifests/"
FILES_DIR = os.getcwd() + "/files/"

file_store = LocalStore(prefix=FILES_DIR)
file_registry = ObjectStoreRegistry({"files://" + FILES_DIR: file_store})


def download_file(link: str):
    path = FILES_DIR + link.removeprefix("https://data.cresis.ku.edu/data/")
    parent = "/".join(path.split("/")[:-1])
    os.makedirs(parent, exist_ok=True)

    file_registry.register("file://" + path, file_store)

    if os.path.exists(path):
        return "file://" + path

    r = requests.get(link)
    with open(path, 'wb') as f:
        f.write(r.content)

    return "file://" + path


def get_variables(file_path: str):
    f = h5py.File(file_path.removeprefix("file://"), "r")

    file_vars = list(f)
    f.close()

    return file_vars


def iterate_opr_dataproducts():
    """Iterate all the OPR dataproduct download links."""
    if TEST_URL:
        yield TEST_URL
    else:
        opr = xopr.OPRConnection()
        stac_items = opr.query_frames()

        for idx, stac_item in list(stac_items.iterrows())[:100]:
            yield stac_item.assets["CSARP_standard"]["href"]


def create_manifest(file_path: str, parser):
    """Create a manifest file from a dataproduct link."""

    vds = virtualizarr.open_virtual_dataset(
        url=file_path,
        parser=parser,
        registry=file_registry,
        # loadable_variables=['time'],
        decode_times=True,
    )

    vds.virtualize.to_kerchunk('opr.json', format="json")
    ds = xr.open_dataset("opr.json", engine="kerchunk")

    print(ds)
    input("SUCCESSFUL DATASET OPEN")


def is_drop(v: str):
    """Determine if a variable is a drop variable based on its name."""
    if v.startswith("_"):
        return True
    if v.startswith("#"):
        return True
    if "api_key" in v:
        return True

    return False


if __name__ == "__main__":

    for link in iterate_opr_dataproducts():
        file_path = download_file(link)
        file_vars = get_variables(file_path)
        print(f"Trying {link} with vars {file_vars}")
        drop_vars = [v for v in file_vars if is_drop(v)]

        while True:
            parser = virtualizarr.parsers.HDFParser(drop_variables=drop_vars)

            try:
                create_manifest(file_path, parser)
                break
            except ValueError as e:
                new_vars = [a.strip("}{'") for a in str(e).split(" ")[-1].strip("/").split("/")]
                if all(a in drop_vars for a in new_vars):
                    print(f"{e}: No new vars to drop. Skipping")
                    break
                print(f"{e}: Adding vars to drop vars: {new_vars}")
                drop_vars.extend(new_vars)
