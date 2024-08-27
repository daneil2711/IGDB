SELECT 
      id AS idGame,
      explode(franchises) AS idFranchise

FROM bronze_igdb.games
WHERE franchises IS NOT null