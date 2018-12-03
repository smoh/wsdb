import os
import warnings
import records

__all__ = ['wsdb']

curdir = os.path.dirname(os.path.abspath(__file__))

warnings.filterwarnings('ignore', category=UserWarning)

with open(os.path.join(curdir, 'credentials.txt'), 'r') as f:
    user, pw = f.readlines()
    user, pw = user.strip(), pw.strip()
    warnings.simplefilter('ignore')
    wsdb = records.Database(
        "postgres://{user}:{pw}@cappc127.ast.cam.ac.uk/wsdb".format(
        user=user, pw=pw
    ))