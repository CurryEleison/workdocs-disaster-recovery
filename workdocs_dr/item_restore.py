
import datetime
from os import utime
from time import time
from pathlib import Path

from workdocs_dr.document import DocumentHelper


def scribble_file(dirpath, mainrequest, writer, headrequest=None):
    # Note:
    # If headrequest is absent we just download the file
    # If metadata indicates the file is already on disk we return early
    #
    # If headrequest is same as main request it's a large file and
    # TODO:
    # - Prevent overwrites of newer files
    # - Implement injection of documentpath generation (to support lost+found etc)
    # - React if filename is invalid and write to lost+found instead
    # - Fix requesting. Test if file exists first and do a head request if it does

    # Do a head request to check metadata if we have one
    response = headrequest() if headrequest is not None else mainrequest()
    metadata = DocumentHelper.document_metadata_s32dict(response["Metadata"])
    name = metadata["LatestVersionMetadata"]["Name"]
    modified_timestamp = metadata["LatestVersionMetadata"]["ContentModifiedTimestamp"].timestamp()
    documentpath = dirpath / name
    # If metadata in response indicate the file is already on disk we can return early
    if documentpath.exists():
        doc_stat = documentpath.stat()
        if doc_stat.st_size == metadata["LatestVersionMetadata"]["Size"] \
                and abs(doc_stat.st_mtime - modified_timestamp) < 2.0:
            return {"Metadata": metadata, "Path": documentpath, "Action": "SkippedIdentical"}
    # if mainrequest and headrequest are same we can skip the download, so can reuse the response
    # also, already did the download if there isn't a headrequest, so can reuse the response
    bodyresponse = mainrequest() if headrequest is not None and mainrequest != headrequest else response
    with open(documentpath, "wb") as f:
        writer(bodyresponse, f)
    utime(documentpath, (time(), modified_timestamp))
    return {"Metadata": metadata, "Path": documentpath, "Action": "Restored"}
