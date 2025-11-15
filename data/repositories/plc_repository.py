from __future__ import annotations

from sqlalchemy import text

from business.core.plc_data import PlcMessage
from data.repositories.db_connection_helper import get_engine


class PLCRepository:

    def __init__(self) -> None:
        self._engine = get_engine()

    def _row_to_plc_message(self, row) -> PlcMessage:
        """
        Map een DB-row naar PlcMessage.
        Zorg dat de keys overeenkomen met de SELECT-aliasen.
        """
        return PlcMessage(
            id=row["id"],
            error_message=row["error_message"],
            error_nbr=row["error_nbr"],
            error_type=row["error_type"],
            bs_comment=row["bs_comment"],
            vw_mnemonic=row["vw_mnemonic"],
            timestamp=row["timestamp"],
            # plc_messages laat je standaard leeg; kan later gevuld worden indien nodig
        )

    def all_plc(self) -> list[PlcMessage]:
        """
        Haal alle PLC-berichten op.
        Pas 'PlcMessages' en kolomnamen aan jouw schema.
        """
        sql = text(
            """
            SELECT
                id               AS id,
                error_message    AS error_message,
                error_nbr        AS error_nbr,
                error_type       AS error_type,
                bs_comment       AS bs_comment,
                vw_mnemonic      AS vw_mnemonic,
                timestamp        AS timestamp
            FROM PlcMessages
            ORDER BY timestamp DESC
            """
        )

        with self._engine.connect() as conn:
            result = conn.execute(sql)
            rows = result.mappings().all()

        return [self._row_to_plc_message(r) for r in rows]

    def get_plc_message(self, id: float) -> PlcMessage | None:
        """
        Haal één PLC-bericht op aan de hand van een ID.
        """
        sql = text(
            """
            SELECT
                id               AS id,
                error_message    AS error_message,
                error_nbr        AS error_nbr,
                error_type       AS error_type,
                bs_comment       AS bs_comment,
                vw_mnemonic      AS vw_mnemonic,
                timestamp        AS timestamp
            FROM PlcMessages
            WHERE id = :id
            """
        )

        with self._engine.connect() as conn:
            row = conn.execute(sql, {"id": id}).mappings().one_or_none()

        if row is None:
            return None

        return self._row_to_plc_message(row)