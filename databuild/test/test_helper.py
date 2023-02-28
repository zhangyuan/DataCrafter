import os


def fixture_path(path):
    parent_path = os.path.dirname(__file__)
    return os.path.join(parent_path, path)
