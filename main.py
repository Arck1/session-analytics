import datetime
import json
import os

from sqlalchemy import create_engine, func, and_
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as db
from sqlalchemy.orm import sessionmaker

WAIT_DELTA = 900  # 15 min
BETWEEN_DELTA = 1800  # 30 min

Base = declarative_base()
engine = None
db_session = None
db_session_maker = None


def get_engine():
    MYSQL_USER = os.environ.get("_MYSQL_USER", "root")
    MYSQL_PASSWORD = os.environ.get("_MYSQL_PASSWORD", "mysecretpassword")
    MYSQL_HOST = os.environ.get("_MYSQL_HOST", "127.0.0.1")
    MYSQL_PORT = os.environ.get("_MYSQL_PORT", "3306")
    DATABASE_NAME = os.environ.get("_DATABASE_NAME", "tetsplanet")
    _engine = create_engine(
        f"mysql+mysqldb://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{DATABASE_NAME}"
        f"?charset=utf8mb4&binary_prefix=true"
    )
    return _engine


def setup_db(engine):
    global db_session_maker
    Base.metadata.create_all(engine)
    db_session_maker = sessionmaker(bind=engine)

    return db_session_maker()


def get_db_session():
    global db_session
    global engine
    if db_session is None:
        db_session = setup_db(engine)
    return db_session


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
        if self.visit_in is not None:
            self.visit_in = datetime.datetime.fromtimestamp(self.visit_in)
        self.time = rec.get('time')
        self.duration = WAIT_DELTA
        self.raw = rec

    @property
    def visit(self):
        return self.visit_in.timestamp()

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


def upload_sessions(sessions: list):
    """
    Save SessionDB object to database
    :param sessions:
    :return:
    """
    _sess = get_db_session()
    try:
        _sess.add_all(sessions)
        _sess.commit()
        print(len(sessions), "added to database")
    except Exception as e:
        _sess.rollback()
        raise


def get_last_sessions():
    """
    SELECT
        *
    FROM (
        SELECT
            testplanet.session.crc,
            MAX(testplanet.session.visit_in) AS visit
        FROM testplanet.session
        GROUP BY testplanet.session.crc
    ) T1
    JOIN testplanet.session T2
    ON T1.crc = T2.crc
    AND T1.visit = T2.visit_in;
    :return:
    """
    _sess = get_db_session()
    try:
        subquery = (_sess.query(SessionDB.crc,
                                func.max(SessionDB.visit_in).label("visit_in")
                                )
                    .group_by(SessionDB.crc)
                    .subquery()
                    )

        sessions = (_sess.query(SessionDB)
                    .join(subquery,
                          and_(SessionDB.crc == subquery.c.crc,
                               SessionDB.visit_in == subquery.c.visit_in)
                          )
                    .distinct(SessionDB.crc)
                    .all()
                    )
    except Exception as e:
        print(e)
        raise
    return sessions


def get_client_statistics():
    """
    select
           T.crc,
           count(*) as sessions_count,
           sum(T.duration) as sum_time
    from testplanet.session T group by T.crc;
    :return:
    """
    _sess = get_db_session()
    try:
        stats = (_sess.query(SessionDB.crc,
                             func.count(SessionDB.visit_in).label("sessions_count"),
                             func.sum(SessionDB.duration).label("sum_time")
                             )
                 .group_by(SessionDB.crc)
                 .all()
                 )
    except Exception as e:
        print(e)
        raise
    return stats


def solve(records: list, last_sessions: list = None) -> dict:
    sessions = {}
    if last_sessions is not None:
        sessions = {s.crc: s for s in last_sessions}
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

        if len(old_sessions) >= 1000:
            upload_sessions(old_sessions)
            old_sessions = []

    upload_sessions(old_sessions + list(sessions.values()))

    return sessions


def main(file_name):
    with open(file_name) as file:
        data = json.load(file)
        solve(data, get_last_sessions())

    stat = get_client_statistics()
    print(len(stat))
    print(list(stat))


if __name__ == '__main__':
    engine = get_engine()
    db_session = get_db_session()
    FILE_NAME = os.environ.get("FILE_NAME", 'test.json')
    main(FILE_NAME)
