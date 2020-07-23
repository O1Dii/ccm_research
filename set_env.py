import os


def set_env_from_env_file(filename='.env'):
    try:
        with open(filename, 'r') as f:
            for line in f:
                name, val = line.rstrip('\n').split('=', 1)
                os.environ[name] = val
    except FileNotFoundError:
        pass