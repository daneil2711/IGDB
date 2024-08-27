SELECT id AS idGame,
       explode(dlcs) AS idGameDLC

FROM bronze_igdb.games

WHERE dlcs IS NOT null