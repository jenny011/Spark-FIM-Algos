import os

def get_DB(DBDIR, dbname):
    if dbname == "retail":
        DBFILENAME = "retail.txt"
    elif dbname == "kosarak":
        DBFILENAME = "kosarak.txt"
    elif dbname == "chainstore":
        DBFILENAME = "chainstoreFIM.txt"
    elif dbname == "susy":
        DBFILENAME = "SUSY.txt"
    elif dbname == "record":
        DBFILENAME = "RecordLink.txt"
    elif dbname == "skin":
        DBFILENAME = "Skin.txt"
    elif dbname == "uscensus":
        DBFILENAME = "USCensus.txt"
    elif dbname == "online":
        DBFILENAME = "OnlineRetailZZ.txt"
    elif dbname == "test":
        DBFILENAME = "transData.txt"
    return scanDB(os.path.join(DBDIR, DBFILENAME), " ")


def scanDB(fpath, delimiter):
    db = []
    with open(fpath, 'r') as f:
        for line in f:
            if line:
                db.append(line)
    return db

def writeDB(dir, db, number):
    with open(os.path.join(dir, f"db_{number}.txt"), 'w') as f:
        for line in db:
            f.write(line)
