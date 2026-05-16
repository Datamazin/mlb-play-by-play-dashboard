-- ============================================================
-- MLB Play-by-Play Pipeline — SQL Schema
-- Target: Azure SQL Database
-- ============================================================

-- ── 1. Plays table ───────────────────────────────────────────
IF NOT EXISTS (
    SELECT 1 FROM sys.tables
    WHERE object_id = OBJECT_ID('dbo.mlb_plays')
)
BEGIN
    CREATE TABLE dbo.mlb_plays (
        id            INT            IDENTITY(1,1) PRIMARY KEY,
        game_pk       INT            NOT NULL,
        game_date     DATE           NOT NULL,
        at_bat_index  INT            NOT NULL,
        inning        TINYINT        NOT NULL,
        half_inning   VARCHAR(6)     NOT NULL,   -- 'top' | 'bottom'
        is_complete   BIT            NOT NULL DEFAULT 0,
        event         VARCHAR(100)   NULL,
        event_type    VARCHAR(100)   NULL,
        description   NVARCHAR(500)  NULL,
        rbi           TINYINT        NULL,
        away_score    TINYINT        NULL,
        home_score    TINYINT        NULL,
        batter_id     INT            NULL,
        batter_name   VARCHAR(100)   NULL,
        pitcher_id    INT            NULL,
        pitcher_name  VARCHAR(100)   NULL,
        ingested_at   DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),
        updated_at    DATETIME2      NOT NULL DEFAULT SYSUTCDATETIME(),

        CONSTRAINT UQ_mlb_plays_game_atbat UNIQUE (game_pk, at_bat_index)
    );

    CREATE NONCLUSTERED INDEX IX_mlb_plays_game_date
        ON dbo.mlb_plays (game_date, game_pk);

    CREATE NONCLUSTERED INDEX IX_mlb_plays_batter
        ON dbo.mlb_plays (batter_id)
        INCLUDE (event, rbi, inning);

    CREATE NONCLUSTERED INDEX IX_mlb_plays_pitcher
        ON dbo.mlb_plays (pitcher_id)
        INCLUDE (event, inning);

    PRINT 'Created table dbo.mlb_plays';
END
GO

-- ── 2. Table-valued parameter type (used by ADF upsert SP) ───
IF NOT EXISTS (
    SELECT 1 FROM sys.types WHERE name = 'PlayTableType'
)
BEGIN
    CREATE TYPE dbo.PlayTableType AS TABLE (
        game_pk       INT,
        game_date     DATE,
        at_bat_index  INT,
        inning        TINYINT,
        half_inning   VARCHAR(6),
        is_complete   BIT,
        event         VARCHAR(100),
        event_type    VARCHAR(100),
        description   NVARCHAR(500),
        rbi           TINYINT,
        away_score    TINYINT,
        home_score    TINYINT,
        batter_id     INT,
        batter_name   VARCHAR(100),
        pitcher_id    INT,
        pitcher_name  VARCHAR(100)
    );
    PRINT 'Created type dbo.PlayTableType';
END
GO

-- ── 3. Upsert stored procedure (called by ADF Copy sink) ─────
CREATE OR ALTER PROCEDURE dbo.usp_UpsertMLBPlays
    @plays dbo.PlayTableType READONLY
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.mlb_plays AS target
    USING @plays AS source
    ON  target.game_pk      = source.game_pk
    AND target.at_bat_index = source.at_bat_index

    WHEN MATCHED THEN UPDATE SET
        target.is_complete   = source.is_complete,
        target.event         = source.event,
        target.event_type    = source.event_type,
        target.description   = source.description,
        target.rbi           = source.rbi,
        target.away_score    = source.away_score,
        target.home_score    = source.home_score,
        target.batter_id     = source.batter_id,
        target.batter_name   = source.batter_name,
        target.pitcher_id    = source.pitcher_id,
        target.pitcher_name  = source.pitcher_name,
        target.updated_at    = SYSUTCDATETIME()

    WHEN NOT MATCHED BY TARGET THEN INSERT (
        game_pk, game_date, at_bat_index, inning, half_inning,
        is_complete, event, event_type, description, rbi,
        away_score, home_score, batter_id, batter_name,
        pitcher_id, pitcher_name
    ) VALUES (
        source.game_pk, source.game_date, source.at_bat_index,
        source.inning, source.half_inning, source.is_complete,
        source.event, source.event_type, source.description,
        source.rbi, source.away_score, source.home_score,
        source.batter_id, source.batter_name,
        source.pitcher_id, source.pitcher_name
    );

    PRINT CONCAT(@@ROWCOUNT, ' row(s) affected');
END
GO

-- ── 4. Handy monitoring views ─────────────────────────────────

-- Live scoreboard for today's games
CREATE OR ALTER VIEW dbo.vw_live_scores AS
SELECT
    game_pk,
    game_date,
    MAX(away_score) AS away_score,
    MAX(home_score) AS home_score,
    MAX(inning)     AS current_inning,
    COUNT(*)        AS total_plays
FROM dbo.mlb_plays
WHERE game_date = CAST(SYSUTCDATETIME() AS DATE)
GROUP BY game_pk, game_date;
GO

-- Recent plays (last 10 minutes across all active games)
CREATE OR ALTER VIEW dbo.vw_recent_plays AS
SELECT TOP 200
    game_pk, game_date, inning, half_inning,
    batter_name, pitcher_name,
    event, description,
    away_score, home_score,
    ingested_at, updated_at
FROM dbo.mlb_plays
WHERE ingested_at >= DATEADD(MINUTE, -10, SYSUTCDATETIME())
ORDER BY updated_at DESC;
GO

PRINT 'Schema deployment complete.';
