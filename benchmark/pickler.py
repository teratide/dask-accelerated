import pickle
from datetime import datetime

data_root = '../notebooks/'


# Data must be a dict, stores to notebook folder for further processing
def store_to_notebooks(data):

    timestamp = datetime.now().strftime("%d-%b-%Y_%H:%M:%S")

    # Add timestamp to data
    data['timestamp'] = timestamp

    with open(data_root + 'data.pickle', 'wb') as f:
        pickle.dump(data, f, pickle.HIGHEST_PROTOCOL)


def load_from_notebooks():

    with open(data_root + 'data.pickle', 'rb') as f:
        data = pickle.load(f)

    return data