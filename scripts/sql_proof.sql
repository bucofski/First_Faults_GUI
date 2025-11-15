CREATE TABLE plc_message (
    id             BIGINT IDENTITY(1,1) PRIMARY KEY,
    error_message  NVARCHAR(MAX)      NOT NULL,
    error_nbr      NVARCHAR(64)       NOT NULL,
    error_type     NVARCHAR(64)       NOT NULL,
    bs_comment     NVARCHAR(MAX)      NULL,
    vw_mnemonic    NVARCHAR(128)      NULL,
    timestamp_utc  DATETIME2(3)       NOT NULL
        CONSTRAINT DF_plc_message_timestamp_utc DEFAULT (SYSUTCDATETIME())
);
GO

-- Self-referencing relation: one message can have many child messages
CREATE TABLE plc_message_relation (
    parent_id  BIGINT NOT NULL,
    child_id   BIGINT NOT NULL,
    CONSTRAINT PK_plc_message_relation
        PRIMARY KEY (parent_id, child_id),
    CONSTRAINT FK_plc_message_relation_parent
        FOREIGN KEY (parent_id) REFERENCES plc_message (id)
        ON DELETE CASCADE,
    CONSTRAINT FK_plc_message_relation_child
        FOREIGN KEY (child_id) REFERENCES plc_message (id)
        ON DELETE NO ACTION,
    CONSTRAINT CHK_plc_message_relation_parent_not_child
        CHECK (parent_id <> child_id)
);

DECLARE @counter INT = 1;
WHILE @counter <= 20
    BEGIN
        INSERT INTO plc_message (error_message, error_nbr, error_type, bs_comment, vw_mnemonic)
        VALUES ('Parent Error Message ' + CAST(@counter AS NVARCHAR(10)),
                'ERR-P-' + RIGHT('000' + CAST(@counter AS NVARCHAR(3)), 3),
                'ERROR',
                'Parent Comment ' + CAST(@counter AS NVARCHAR(10)),
                'PARENT_' + CAST(@counter AS NVARCHAR(10)));

        -- Get the parent ID
        DECLARE @parent_id BIGINT = SCOPE_IDENTITY();

        -- Generate 20 child messages for each parent
        DECLARE @child_counter INT = 1;
        WHILE @child_counter <= 20
            BEGIN
                -- Insert child message
                INSERT INTO plc_message (error_message, error_nbr, error_type, bs_comment, vw_mnemonic)
                VALUES ('Child Error Message ' + CAST(@counter AS NVARCHAR(10)) + '-' +
                        CAST(@child_counter AS NVARCHAR(10)),
                        'ERR-C-' + RIGHT('000' + CAST(@child_counter AS NVARCHAR(3)), 3),
                        'WARNING',
                        'Child Comment ' + CAST(@counter AS NVARCHAR(10)) + '-' + CAST(@child_counter AS NVARCHAR(10)),
                        'CHILD_' + CAST(@counter AS NVARCHAR(10)) + '_' + CAST(@child_counter AS NVARCHAR(10)));

                -- Create relation between parent and child
                INSERT INTO plc_message_relation (parent_id, child_id)
                VALUES (@parent_id, SCOPE_IDENTITY());

                SET @child_counter = @child_counter + 1;
            END

        SET @counter = @counter + 1;
    END
GO






DECLARE @id BIGINT = 123;  -- example parent id

SELECT
    p.id,
    p.error_message,
    p.error_nbr,
    p.error_type,
    p.bs_comment,
    p.vw_mnemonic,
    p.timestamp_utc,
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
WHERE p.id = @id;