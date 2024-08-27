SELECT id AS idGame,
       explode(expansions) AS idGameExpansion

FROM bronze_igdb.games

WHERE expansions IS NOT null 