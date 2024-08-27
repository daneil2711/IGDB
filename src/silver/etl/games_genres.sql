SELECT id AS idGame,
       explode(genres) AS idGenre

FROM bronze_igdb.games
WHERE genres IS NOT NULL