import datetime
import logging
from typing import List, Type

from sqlalchemy import and_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import joinedload, Session

from db.models import Debtor


class DebtorCrud:
    def __init__(self, session):
        self.session: Session = session

    def get_by_identifier(self, identifier: str) -> List[Type[Debtor]]:
        return self.session.query(Debtor).options(
            joinedload(Debtor.debt_type),
        ).filter_by(identifier=identifier.all)

    def create_status(self, debtor_obj) -> Debtor:
        model_dict = debtor_obj.model_dump()
        status_obj = Debtor(**model_dict)
        self.session.add(status_obj)
        self.session.commit()
        self.session.refresh(status_obj)
        return status_obj

    def delete_status(self, id: int = None, identifier: str = None, debt_type_id: int = None) -> int:
        filters = []
        if id:
            filters.append(Debtor.id == id)
        if identifier:
            filters.append(Debtor.identifier == identifier)
        if debt_type_id:
            filters.append(Debtor.debt_type_id == debt_type_id)
        if not filters:
            raise ValueError('At least one field is required (id, identifier, debt_type_id)')
        deleted_rows = self.session.query(Debtor).filter(*filters).delete()
        self.session.commit()
        return deleted_rows

    def check_duplicate_debtor(self, app_num: str, fio: str) -> bool:
        return self.session.query(Debtor).filter(
            Debtor.app_num == app_num,
            Debtor.fio == fio
        ).first() is not None

    def bulk_upsert(self, data: List[dict]):
        try:
            insert_stmt = insert(Debtor).values(data)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                set_={
                    'last_updated': datetime.datetime.now(),
                },
                constraint='uq_has_value',
            )

            self.session.execute(upsert_stmt)
            self.session.commit()


        except Exception as e:
            logging.exception(e)

    def delete_all(self, to_date: str, type_id: int):
        deleted_count = self.session.query(Debtor).filter(
            and_(
                Debtor.last_updated < to_date,
                Debtor.debt_type_id == type_id,
            )
        ).delete(synchronize_session=False)
        self.session.commit()
        return deleted_count

    def close(self):
        self.session.close()
