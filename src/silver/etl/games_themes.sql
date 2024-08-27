SELECT id AS idGame,
       explode(themes) AS idTheme

FROM bronze_igdb.games