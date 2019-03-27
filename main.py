import datetime
import json

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker

WAIT_DELTA = 900
BETWEEN_DELTA = 1800

Base = declarative_base()
engine = None
db_session = None


class SessionDB(Base):
    __tablename__ = 'session'

    id = db.Column(db.Integer, primary_key=True)
    client = db.Column(db.Integer)
    crc = db.Column(db.BigInteger)
    elite = db.Column(db.BigInteger)
    visit_in = db.Column(db.DateTime)
    time = db.Column(db.String(256))
    duration = db.Column(db.Integer)

    def __init__(self, rec: dict):
        self.client = rec.get('client')
        self.crc = rec.get('crc')
        self.elite = rec.get('elite')
        self.visit_in = rec.get('visit_in')
        self.visit = self.visit_in
        if self.visit_in:
            self.visit_in = datetime.datetime.fromtimestamp(self.visit_in)
        self.time = rec.get('time')
        self.duration = WAIT_DELTA
        self.raw = rec

    def __str__(self):
        return str({
            "id": self.id,
            "client": self.client,
            "crc": self.crc,
            "elite": self.elite,
            "visit_in": self.visit_in,
            "time": self.time,
            "duration": self.duration,
        })

    def __repr__(self):
        return str({
            "id": self.id,
            "client": self.client,
            "crc": self.crc,
            "elite": self.elite,
            "visit_in": self.visit_in,
            "time": self.time,
            "duration": self.duration,
        })


def get_engine():
    engine = create_engine(
        "mysql+mysqldb://root:mysecretpassword@127.0.0.1/testplanet?charset=utf8mb4&binary_prefix=true"
    )
    return engine


def setup_db(engine):
    Base.metadata.create_all(engine)
    db_session = sessionmaker(bind=engine)()

    return db_session


class SessionFake:
    __slots__ = [
        "client",
        "crc",
        "elite",
        "visit_in",
        "time",
        "duration",
        "raw"
    ]

    def __init__(self, rec: dict):
        self.client = rec.get('client')
        self.crc = rec.get('crc')
        self.elite = rec.get('elite')
        self.visit_in = rec.get('visit_in')
        self.time = rec.get('time')
        self.duration = WAIT_DELTA
        self.raw = rec


def fix_key_error(data: dict) -> dict:
    return data


def upload_sessions(sessions: list):
    try:
        db_session.add_all(sessions)
        db_session.commit()
    except Exception as e:
        db_session.rollback()
        raise


def calculate_sessions(records: list) -> dict:
    sessions = {}
    old_sessions = []

    for record in records:
        crc, visit_in = record.get("crc"), record.get("visit_in")
        if crc is None or visit_in is None:
            continue
        if crc not in sessions:
            sessions[crc] = SessionDB(record)
        session = sessions.get(crc)

        if session.visit + session.duration + BETWEEN_DELTA >= visit_in:
            session.duration = max(session.visit + session.duration, visit_in + WAIT_DELTA) - session.visit
        else:
            old_sessions.append(session)
            sessions[crc] = SessionDB(record)
    upload_sessions(old_sessions + list(sessions.values()))

    return sessions


def main(file_name):

    # s = db_session.query(SessionDB).all()
    # print(s)
    # new_s = SessionDB({'id': 1, 'client': 1, 'crc': 111, 'elite': 0, 'visit_in': datetime.datetime(1970, 1, 1, 0, 0), 'time': '5', 'duration': 55})
    #
    # db_session.add(new_s)
    # s = db_session.query(SessionDB).all()
    # db_session.commit()
    # print(s)

    with open(file_name) as file:
        data = json.load(file)
        calculate_sessions(data)


if __name__ == '__main__':
    engine = get_engine()
    db_session = setup_db(engine)
    main('test.json')

