SELECT id AS idGame,
       explode(platforms) as idPlatform

FROM bronze_igdb.games
WHERE platforms IS NOT null