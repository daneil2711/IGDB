SELECT 
      from_unixtime(created_at) AS dtCreated,
      id AS idGenre,
      name AS descName,
      slug AS descSlug,
      from_unixtime(updated_at) AS dtUpdated,
      url AS urlGenre

FROM bronze_igdb.genres