from __future__ import annotations

from sqlalchemy import text
import json
from business.core.plc_data import PlcMessage
from data.repositories.db_connection_helper import get_engine


class PLCRepository:

    def __init__(self) -> None:
        self._engine = get_engine()

    def _row_to_plc_message(self, row, plc_messages: list[PlcMessage] | None = None) -> PlcMessage:
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
            plc_messages=plc_messages or [],
        )

    def all_plc(self) -> list[PlcMessage]:
        """
        Haal alle PLC-berichten op.
        Pas kolomnamen aan jouw schema.
        """
        sql = text(
            """
            SELECT
                id              AS id,
                error_message   AS error_message,
                error_nbr       AS error_nbr,
                error_type      AS error_type,
                bs_comment      AS bs_comment,
                vw_mnemonic     AS vw_mnemonic,
                timestamp_utc   AS [timestamp]
            FROM plc_message
            ORDER BY timestamp_utc DESC
            """
        )

        with self._engine.connect() as conn:
            result = conn.execute(sql)
            rows = result.mappings().all()

        return [self._row_to_plc_message(r) for r in rows]

    def get_plc_message(self, id: float) -> PlcMessage | None:
        """
        Haal één PLC-bericht op aan de hand van een ID, inclusief child PLC-berichten.
        """
        sql = text(
            """
            SELECT
                p.id,
                p.error_message,
                p.error_nbr,
                p.error_type,
                p.bs_comment,
                p.vw_mnemonic,
                p.timestamp_utc AS [timestamp],
                ISNULL(
                    (
                        SELECT
                            c.id,
                            c.error_message,
                            c.error_nbr,
                            c.error_type,
                            c.bs_comment,
                            c.vw_mnemonic,
                            c.timestamp_utc AS [timestamp]
                        FROM plc_message_relation AS r
                        INNER JOIN plc_message AS c
                            ON c.id = r.child_id
                        WHERE r.parent_id = p.id
                        FOR JSON PATH
                    ),
                    '[]'
                ) AS plc_messages
            FROM plc_message AS p
            WHERE p.id = :id
            """
        )

        with self._engine.connect() as conn:
            row = conn.execute(sql, {"id": id}).mappings().one_or_none()

        if row is None:
            return None


        raw_children = row["plc_messages"] or "[]"
        children_payload = json.loads(raw_children)

        children: list[PlcMessage] = [
            self._row_to_plc_message(child_row, plc_messages=[])
            for child_row in children_payload
        ]

        return self._row_to_plc_message(row, plc_messages=children)