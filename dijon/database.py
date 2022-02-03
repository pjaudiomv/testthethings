from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from dijon.settings import settings


Base = declarative_base()


def get_db_url():
    db_user = settings.get("DBUSER", "dijon")
    db_pass = settings.get("DBPASSWORD", "dijon")
    db_host = settings.get("DBHOST", "0.0.0.0")
    db_port = settings.get("DBPORT", 3306)
    db_name = settings.get("DBNAME", "dijon")
    return f"mysql+mysqlconnector://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"


engine = create_engine(get_db_url())
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


@contextmanager
def db_context():
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except:  # noqa: E722
        session.rollback()
        raise
    finally:
        session.close()
