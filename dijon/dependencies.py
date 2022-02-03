from fastapi import Depends
from sqlalchemy.orm import Session

from dijon.database import SessionLocal


def get_db():
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except:  # noqa: E722
        db.rollback()
        raise
    finally:
        db.close()


class Context:
    def __init__(self, db: Session = Depends(get_db)):
        self.db = db
