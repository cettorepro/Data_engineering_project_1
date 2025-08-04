--Suppression des doublons absolus
delete from housing_aus as f where f.idh in (
SELECT DISTINCT h.idh
FROM  housing_aus as h
WHERE EXISTS (
              SELECT * 
              FROM  housing_aus a
              WHERE h.idh <> a.idh
              AND   h.address = a.address
              AND   h.lattitude =  a.lattitude
              AND    h.longtitude =  a.longtitude
              and h.suburb = a.suburb 
              and h.distance = a.distance
              and h.regionh = a.regionh
              and a.landsize = h.landsize
              and a.monthh = h.monthh
              and a.yearh = h.yearh
              and a.dayh = h.dayh
                 ))

