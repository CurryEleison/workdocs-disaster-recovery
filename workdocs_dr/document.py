import logging
from string import capwords
import base64
import datetime
import re


class DocumentHelper():

    FOLDERINFONAME = ".folderinfo"
    USERINFONAME = ".userinfo"

    @staticmethod
    def metadata_dict2s3(wdmetadata):
        snake_case_key = {k: DocumentHelper.pascal_to_snakecase(k) for k in wdmetadata.keys()}
        items = wdmetadata.items()
        asciistrs = {k: v for k, v in items if isinstance(v, str) and v.isascii()}
        nonasciistrs = {k: base64.b64encode(v.encode("utf-8")).decode('ascii')
                        for k, v in items if isinstance(v, str) and not v.isascii()}
        dates = {k: v.astimezone(datetime.timezone.utc).isoformat()
                 for k, v in items if isinstance(v, datetime.datetime)}
        ints = {k: str(v) for k, v in items if isinstance(v, (int, float, complex))}
        return {**{snake_case_key[k]: v for k, v in {**asciistrs, **dates, **ints}.items()}, **{f"base64_{snake_case_key[k]}": v for k, v in nonasciistrs.items()}}

    @staticmethod
    def metadata_s32dict(s3metadata):
        items = s3metadata.items()
        keymap = {k: DocumentHelper.snakecase_to_pascal(DocumentHelper.removeprefix(k, "base64_")) \
            for k in s3metadata.keys()}
        base64items = {k: base64.b64decode(v).decode('utf-8') for k, v in items if k.startswith("base64_")}
        dates = {k: v for k, v in [(k, DocumentHelper.datetime_valid(v)) for k, v in items] if v is not None}
        numbers = {k: int(v) if float(v).is_integer() else float(v) for k, v in items if str.isnumeric(v)}
        logging.debug(base64items)
        # asciistrs = {k: v for k, v in s3metadataitems.items() if k not in base64items.keys() and k not in dates.keys() and k not in numbers.keys()}
        return {keymap[k]: v for k, v in {**s3metadata, **base64items, **dates, **numbers}.items()}

    @staticmethod
    def removeprefix(str, prefix):
        return str[len(prefix):] if str.startswith(prefix) else str

    @staticmethod
    def document_metadata_s32dict(s3metadataitems):
        flatdict = {DocumentHelper.document_metadata_keys_patches(
            k): v for k, v in DocumentHelper.metadata_s32dict(s3metadataitems).items()}
        latestversionkeys = {"Name", "ContentType", "Size", "Signature",
                             "Status", "ContentCreatedTimestamp", "ContentModifiedTimestamp"}
        metadata = {k: v for k, v in flatdict.items() if k not in latestversionkeys}
        metadata["LatestVersionMetadata"] = {k: v for k, v in flatdict.items() if k in latestversionkeys}
        return metadata

    @staticmethod
    def folder_metadata_s32dict(s3metadataitems):
        return {DocumentHelper.folder_metadata_keys_patches(k): v for k, v in DocumentHelper.metadata_s32dict(s3metadataitems).items()}

    @staticmethod
    def user_metadata_s32dict(s3metadataitems):
        return {DocumentHelper.usermetadata_keys_patches(k): v for k, v in DocumentHelper.metadata_s32dict(s3metadataitems).items()}

    @staticmethod
    def datetime_valid(dt_str):
        try:
            return datetime.datetime.fromisoformat(dt_str)
        except:
            return None

    @staticmethod
    def snakecase_to_pascal(snakecase_string):
        pascal = capwords(snakecase_string.replace('_', ' '))
        pascal = pascal.replace(' ', '')
        return pascal

    @staticmethod
    def pascal_to_snakecase(pascal_string):
        return re.sub(r'(?<!^)(?=[A-Z])', '_', pascal_string).lower()

    @staticmethod
    def folder_metadata_keys_patches(keyname):
        #TODO: Don't think we need these any more
        key = keyname.casefold()
        if key == "creatorid":
            return "CreatorId"
        if key == "parentfolderid":
            return "ParentFolderId"
        if key == "createdtimestamp":
            return "CreatedTimestamp"
        if key == "modifiedtimestamp":
            return "ModifiedTimestamp"
        if key == "resourcestate":
            return "ResourceState"
        if key == "latestversionsize":
            return "LatestVersionSize"
        return keyname

    @staticmethod
    def document_metadata_keys_patches(keyname):
        #TODO: Don't think we need these any more
        key = keyname.casefold()
        if key == "contenttype":
            return "ContentType"
        if key == "contentmodifiedtimestamp":
            return "ContentModifiedTimestamp"
        if key == "contentcreatedtimestamp":
            return "ContentCreatedTimestamp"
        return DocumentHelper.folder_metadata_keys_patches(keyname)

    @staticmethod
    def usermetadata_keys_patches(keyname):
        if keyname == "lastmodified" or keyname == capwords("lastmodified"):
            return "ModifiedTimestamp"
        return keyname
