from sqlalchemy import BigInteger, Column, String, ForeignKey, DateTime, func, UniqueConstraint, Integer
from sqlalchemy.orm import relationship, Mapped

from db.connection import Base, engine
from config import settings


class Debtor(Base):
    __tablename__ = "debtor"
    __table_args__ = (
        (UniqueConstraint('hash_value', name='uq_has_value')),
        {'schema': settings.DB_SCHEMA},
    )

    id = Column(BigInteger, primary_key=True)

    created = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    last_updated = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    identifier = Column(String, nullable=False, index=True)
    debt_type_id = Column(BigInteger, ForeignKey(column=f'{settings.DB_SCHEMA}.debt_type_ref.id', name='fk_debtor_debt_type_id'))
    fio = Column(String)
    category = Column(String)
    provider = Column(String)
    app_num = Column(String)
    app_date = Column(DateTime)
    procedure_start_date = Column(DateTime)
    procedure_end_date = Column(DateTime)
    procedure_stop_num = Column(String)
    decision_date = Column(DateTime)
    decision_start_date = Column(DateTime)
    stop_initiator = Column(String)
    status = Column(String)
    creditors_list = Column(String)
    debt_sum = Column(BigInteger)
    region = Column(String)
    hash_value = Column(String, nullable=False)

    debt_type: Mapped['DebtTypeRef'] = relationship("DebtTypeRef")


class DebtTypeRef(Base):
    __tablename__ = "debt_type_ref"
    __table_args__ = {'schema': settings.DB_SCHEMA}

    id = Column(BigInteger, primary_key=True)
    title = Column(String)


if __name__ == "__main__":
    Base.metadata.create_all(engine)
