SELECT id AS idGameMode,
       name AS descName,
       slug AS descSlug,
       from_unixtime(created_at) AS dtCreated,
       from_unixtime(updated_at) AS dtUpdated

FROM bronze_igdb.game_modes