SELECT id AS idCollection,
       explode(games) AS idGame

FROM bronze_igdb.collections
WHERE games IS NOT null
