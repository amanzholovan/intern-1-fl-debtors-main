from sqlalchemy import BigInteger, Column, String, ForeignKey, DateTime, func, UniqueConstraint
from sqlalchemy.orm import relationship, Mapped, sessionmaker
from sqlalchemy.sql import text
from db.connection import Base, engine
from config import settings

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

class Debtor(Base):
    __tablename__ = "debtor"
    __table_args__ = (
        (UniqueConstraint('hash_value', name='uq_has_value')),
        {'schema': settings.DB_SCHEMA},
    )

    id = Column(BigInteger, primary_key=True, autoincrement=True)
    created = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    last_updated = Column(DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now())

    identifier = Column(String, nullable=True, index=True)
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

    @classmethod
    def create_shadow_table(cls):
        return text(f"""
            CREATE TABLE IF NOT EXISTS {settings.DB_SCHEMA}.debtor_shadow 
            AS TABLE {settings.DB_SCHEMA}.debtor WITH NO DATA;
        """)

class DebtTypeRef(Base):
    __tablename__ = "debt_type_ref"
    __table_args__ = {'schema': settings.DB_SCHEMA}

    id = Column(BigInteger, primary_key=True)
    title = Column(String)

    @staticmethod
    def insert_debt_type_ref_values(session):
        try:
            session.execute(text("""
                INSERT INTO {settings.DB_SCHEMA}.debt_type_ref (id, title)
                VALUES
                    (1, 'В списке заявителей на процедуру внесудебного банкротства'),
                    (2, 'В списке заявителей на процедуру судебного банкротства'),
                    (3, 'Объявления о прекращении процедуры'),
                    (4, 'В реестре банкротов (внесудебная процедура)'),
                    (5, 'В реестре банкротов (судебная процедура)'),
                    (6, 'Восстановление платежеспособности')
                ON CONFLICT (id) DO NOTHING;
            """))
            session.commit()
        except Exception as e:
            print(f"Error inserting debt types: {e}")
            session.rollback()

def shadow_table_creation():
    session = SessionLocal()
    try:
        session.execute(Debtor.create_shadow_table())
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error during shadow table creation: {e}")
    finally:
        session.close()


def insert_debt_type_ref_values():
    session = SessionLocal()
    try:
        DebtTypeRef.insert_debt_type_ref_values(session)
    except Exception as e:
        print(f"Error inserting debt types: {e}")
    finally:
        session.close()


if __name__ == "__main__":
    Base.metadata.create_all(engine)

    insert_debt_type_ref_values()

    shadow_table_creation()
