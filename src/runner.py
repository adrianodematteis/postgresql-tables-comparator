import json
from os.path import dirname, join
from util.compare_tables import compare_tables


def runner():
    # Reading json conf file
    with open(join(dirname(__file__), 'resources', 'config.json',), 'r') as config_file:
        config = json.load(config_file)

    print(compare_tables(config))


if __name__ == "__main__":
    runner()
