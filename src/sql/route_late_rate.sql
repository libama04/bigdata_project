-- Independent recomputation of the per-route late-arrival rate from the RAW csv.
SELECT
    upper(trim(gare_depart))                                           AS gare_depart,
    upper(trim(gare_arrivee))                                          AS gare_arrivee,
    COUNT(DISTINCT "date")                                             AS months_observed,
    SUM(nb_train_prevu - nb_annulation)                                AS trains_circulated,
    SUM(nb_train_retard_arrivee)                                       AS late_arrivals,
    SUM(nb_train_retard_arrivee) / SUM(nb_train_prevu - nb_annulation) AS late_arrival_rate
FROM raw
WHERE nb_train_prevu > 0
  AND nb_annulation <= nb_train_prevu
GROUP BY 1, 2
ORDER BY 1, 2;
