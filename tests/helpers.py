from os import environ


def get_complex_user() -> str:
    return environ.get("WD_COMPLEX_USER")

def get_simple_user() -> str:
    return environ.get("WD_SIMPLE_USER")

def get_known_workdocs_path() -> str:
    """A path from complex_user that we know exists"""
    return environ.get("KNOWN_PATH")

